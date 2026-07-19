import asyncio
import logging
from typing import Dict, Set, List
from fastapi import WebSocket
from sqlalchemy.orm import Session
from app.config import settings
from app.geo_service import geo_service
from app.models import Ride, Driver

logger = logging.getLogger("dispatch.engine")

class ConnectionManager:
    """Manages active WebSocket connections for real-time signaling."""
    def __init__(self):
        # schema: {driver_id: WebSocket}
        self.driver_sockets: Dict[str, WebSocket] = {}
        # schema: {rider_id: WebSocket}
        self.rider_sockets: Dict[str, WebSocket] = {}

    async def connect_driver(self, driver_id: str, websocket: WebSocket):
        await websocket.accept()
        self.driver_sockets[driver_id] = websocket
        logger.info(f"Driver WebSocket connected: {driver_id}")

    def disconnect_driver(self, driver_id: str):
        if driver_id in self.driver_sockets:
            del self.driver_sockets[driver_id]
            logger.info(f"Driver WebSocket disconnected: {driver_id}")

    async def connect_rider(self, rider_id: str, websocket: WebSocket):
        await websocket.accept()
        self.rider_sockets[rider_id] = websocket
        logger.info(f"Rider WebSocket connected: {rider_id}")

    def disconnect_rider(self, rider_id: str):
        if rider_id in self.rider_sockets:
            del self.rider_sockets[rider_id]
            logger.info(f"Rider WebSocket disconnected: {rider_id}")

    async def send_to_driver(self, driver_id: str, message: dict) -> bool:
        socket = self.driver_sockets.get(driver_id)
        if socket:
            try:
                await socket.send_json(message)
                return True
            except Exception as e:
                logger.error(f"Error sending WebSocket to driver {driver_id}: {e}")
                self.disconnect_driver(driver_id)
        return False

    async def send_to_rider(self, rider_id: str, message: dict) -> bool:
        socket = self.rider_sockets.get(rider_id)
        if socket:
            try:
                await socket.send_json(message)
                return True
            except Exception as e:
                logger.error(f"Error sending WebSocket to rider {rider_id}: {e}")
                self.disconnect_rider(rider_id)
        return False

# Global connection manager
connection_manager = ConnectionManager()


class DispatchEngine:
    def __init__(self):
        # Tracks driver exclusions per ride ID to prevent offering the same ride repeatedly to rejecting/timeout drivers
        # Schema: {ride_id: {driver_id_1, driver_id_2}}
        self.exclusions: Dict[int, Set[str]] = {}
        # Tracks dispatch tasks to prevent duplicate dispatch loops on the same ride
        self.active_dispatches: Set[int] = set()


    async def dispatch_ride(self, ride_id: int, db: Session):
        """Asynchronous dispatch loop trying to match the ride with the closest driver."""
        if ride_id in self.active_dispatches:
            logger.info(f"Dispatch already active for ride {ride_id}. Skipping duplicate loop.")
            return
        
        self.active_dispatches.add(ride_id)
        try:
            await self._run_dispatch_loop(ride_id, db)
        finally:
            self.active_dispatches.remove(ride_id)

    async def _run_dispatch_loop(self, ride_id: int, db: Session):
        retry_delay = 4
        max_retries = 10
        retries = 0

        while retries < max_retries:
            # Refresh DB session to get latest ride status
            db.expire_all()
            ride = db.query(Ride).filter(Ride.id == ride_id).first()
            if not ride:
                logger.error(f"Ride {ride_id} not found in database.")
                return

            if ride.status != "REQUESTED":
                logger.info(f"Ride {ride_id} is no longer in REQUESTED state (status: {ride.status}). Exiting dispatch loop.")
                return

            logger.info(f"Dispatching Ride {ride_id}: Pickup ({ride.pickup_lat}, {ride.pickup_lon})")

            # Search nearby drivers
            nearby_drivers = geo_service.get_nearby_drivers(
                lat=ride.pickup_lat,
                lon=ride.pickup_lon,
                radius_km=settings.GEOSEARCH_RADIUS_KM
            )

            # Filter out excluded drivers
            excluded = self.exclusions.get(ride_id, set())
            available_drivers = [d for d in nearby_drivers if d[0] not in excluded]

            if not available_drivers:
                logger.info(f"No available drivers near Ride {ride_id} (found {len(nearby_drivers)} total, {len(excluded)} excluded). Retrying in {retry_delay}s...")
                retries += 1
                await asyncio.sleep(retry_delay)
                continue

            # Pick the closest driver
            closest_driver_id, distance = available_drivers[0]
            logger.info(f"Offering Ride {ride_id} to Driver {closest_driver_id} ({distance:.2f} km away)")

            # Update ride status in database to ASSIGNED
            ride.driver_id = closest_driver_id
            ride.status = "ASSIGNED"
            db.commit()

            # Notify the driver of the offer via WebSocket
            offer_sent = await connection_manager.send_to_driver(
                driver_id=closest_driver_id,
                message={
                    "type": "ride_offer",
                    "ride": ride.to_dict()
                }
            )

            if not offer_sent:
                # If websocket transmission fails (driver offline), exclude them and roll back status
                logger.warning(f"Driver {closest_driver_id} websocket unreachable. Exclude and retry.")
                self.exclude_driver(ride_id, closest_driver_id)
                ride.driver_id = None
                ride.status = "REQUESTED"
                db.commit()
                continue

            # Start background monitoring task for this offer
            asyncio.create_task(self._monitor_offer_timeout(ride_id, closest_driver_id, db))
            return

        # If we reach here, we timed out searching for drivers
        ride = db.query(Ride).filter(Ride.id == ride_id).first()
        if ride and ride.status == "REQUESTED":
            ride.status = "CANCELLED"
            db.commit()
            await connection_manager.send_to_rider(
                rider_id=ride.rider_id,
                message={
                    "type": "ride_failed",
                    "reason": "No drivers available in the area."
                }
            )
            logger.warning(f"Dispatch for Ride {ride_id} failed: No drivers found after {max_retries} attempts.")

    async def _monitor_offer_timeout(self, ride_id: int, driver_id: str, db: Session):
        """Monitors a driver offer. If the driver does not respond, rolls over to the next driver."""
        await asyncio.sleep(settings.DRIVER_OFFER_TIMEOUT_SEC)
        
        # Expire session cache to check database values
        db.expire_all()
        ride = db.query(Ride).filter(Ride.id == ride_id).first()
        
        if ride and ride.status == "ASSIGNED" and ride.driver_id == driver_id:
            logger.info(f"Offer for Ride {ride_id} to Driver {driver_id} timed out.")
            
            # Exclude driver from matching
            self.exclude_driver(ride_id, driver_id)
            
            # Reset ride state
            ride.driver_id = None
            ride.status = "REQUESTED"
            db.commit()
            
            # Send notification to driver that offer expired
            await connection_manager.send_to_driver(
                driver_id=driver_id,
                message={"type": "offer_expired", "ride_id": ride_id}
            )

            # Send notification to rider about continuation
            await connection_manager.send_to_rider(
                rider_id=ride.rider_id,
                message={"type": "dispatch_update", "status": "Searching for another driver..."}
            )
            
            # Trigger dispatch loop again
            await self.dispatch_ride(ride_id, db)

    def exclude_driver(self, ride_id: int, driver_id: str):
        if ride_id not in self.exclusions:
            self.exclusions[ride_id] = set()
        self.exclusions[ride_id].add(driver_id)

    def clear_exclusions(self, ride_id: int):
        if ride_id in self.exclusions:
            del self.exclusions[ride_id]

# Global dispatch engine instance
dispatch_engine = DispatchEngine()
