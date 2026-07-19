import asyncio
import json
import urllib.request
import urllib.parse
import random
import logging
import sys
import websockets

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("simulator")

API_URL = "http://localhost:8000"
WS_URL = "ws://localhost:8000"

# Utility helper to perform HTTP POST requests using standard urllib
def http_post(endpoint: str, data: dict) -> dict:
    url = f"{API_URL}{endpoint}"
    json_data = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=json_data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        logger.error(f"HTTP POST to {url} failed: {e}")
        raise

async def async_http_post(endpoint: str, data: dict) -> dict:
    return await asyncio.to_thread(http_post, endpoint, data)

class DriverSimulator:
    def __init__(self, driver_id: str, name: str, start_lat: float, start_lon: float):
        self.driver_id = driver_id
        self.name = name
        self.lat = start_lat
        self.lon = start_lon
        self.status = "AVAILABLE"
        self.current_ride = None
        self.ws = None

    async def run(self):
        # 1. Register driver with server
        try:
            await async_http_post("/drivers", {
                "id": self.driver_id,
                "name": self.name,
                "vehicle_type": "standard"
            })
            logger.info(f"Registered driver {self.name} ({self.driver_id})")
        except Exception:
            # Assume already registered
            logger.info(f"Driver {self.driver_id} already exists on server.")

        # 2. Establish WebSocket connection for telemetry and dispatching
        uri = f"{WS_URL}/ws/driver/{self.driver_id}"
        
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    self.ws = websocket
                    logger.info(f"[{self.driver_id}] Connected to Dispatch WebSocket")
                    
                    # Run location updates and messages listeners in parallel
                    send_task = asyncio.create_task(self.telemetry_loop())
                    recv_task = asyncio.create_task(self.receive_loop())
                    
                    await asyncio.gather(send_task, recv_task)
            except (websockets.ConnectionClosed, ConnectionRefusedError):
                logger.warning(f"[{self.driver_id}] WebSocket disconnected. Retrying in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"[{self.driver_id}] WebSocket error: {e}")
                await asyncio.sleep(5)

    async def telemetry_loop(self):
        """Simulates periodic driver location telemetry reporting."""
        while self.ws and self.ws.state == websockets.State.OPEN:
            # If the driver has a ride, simulate moving towards the target
            if self.current_ride:
                await self.simulate_movement_step()

            # Random drift when idle to show real-time changes
            elif self.status == "AVAILABLE":
                self.lat += random.uniform(-0.0001, 0.0001)
                self.lon += random.uniform(-0.0001, 0.0001)

            try:
                await self.ws.send(json.dumps({
                    "lat": self.lat,
                    "lon": self.lon
                }))
            except Exception as e:
                logger.error(f"[{self.driver_id}] Error sending telemetry: {e}")
                break
            
            await asyncio.sleep(3)  # telemetry report frequency

    async def receive_loop(self):
        """Listens for dispatch ride offers and timeouts."""
        while self.ws and self.ws.state == websockets.State.OPEN:
            try:
                msg_str = await self.ws.recv()
                msg = json.loads(msg_str)
                await self.handle_message(msg)
            except Exception as e:
                logger.error(f"[{self.driver_id}] Error receiving message: {e}")
                break

    async def handle_message(self, msg: dict):
        msg_type = msg.get("type")
        
        if msg_type == "ride_offer":
            ride = msg["ride"]
            ride_id = ride["id"]
            pickup_lat = ride["pickup_lat"]
            pickup_lon = ride["pickup_lon"]
            
            logger.info(f"\n[OFFER] Driver {self.name} ({self.driver_id}) received offer for Ride #{ride_id}.")
            logger.info(f"   Pickup: ({pickup_lat}, {pickup_lon}) -> Dropoff: ({ride['dropoff_lat']}, {ride['dropoff_lon']})")
            
            # Simulate a brief decision time (1-2s)
            await asyncio.sleep(random.uniform(1.0, 2.0))
            
            # Simulate a 15% rejection rate to showcase the fallback matching algorithm
            should_accept = random.random() > 0.15
            
            if should_accept:
                logger.info(f"[ACCEPT] Driver {self.name} accepted Ride #{ride_id}.")
                try:
                    await async_http_post(f"/rides/{ride_id}/accept", {"driver_id": self.driver_id})
                    self.current_ride = ride
                    self.status = "BUSY"
                except Exception as e:
                    logger.error(f"Failed to accept ride on server: {e}")
            else:
                logger.info(f"[REJECT] Driver {self.name} rejected Ride #{ride_id}.")
                try:
                    await async_http_post(f"/rides/{ride_id}/reject", {"driver_id": self.driver_id})
                except Exception as e:
                    logger.error(f"Failed to reject ride on server: {e}")

        elif msg_type == "offer_expired":
            ride_id = msg.get("ride_id")
            logger.warning(f"[EXPIRED] Offer for Ride #{ride_id} expired before driver response.")
            self.current_ride = None
            self.status = "AVAILABLE"

    async def simulate_movement_step(self):
        """Moves driver towards the pickup location, then to the dropoff location."""
        ride = self.current_ride
        if not ride:
            return

        ride_id = ride["id"]
        status = ride["status"]

        if status == "ACCEPTED":
            # Heading to pickup
            target_lat = ride["pickup_lat"]
            target_lon = ride["pickup_lon"]
            step_desc = "heading to pickup"
        else:
            # Heading to dropoff (IN_PROGRESS)
            target_lat = ride["dropoff_lat"]
            target_lon = ride["dropoff_lon"]
            step_desc = "heading to dropoff"

        # Calculate difference
        d_lat = target_lat - self.lat
        d_lon = target_lon - self.lon
        distance_to_target = math.sqrt(d_lat**2 + d_lon**2)

        # Movement speed factor (approx 0.001 coordinates per step)
        speed = 0.001
        
        if distance_to_target <= speed:
            # Arrived at target
            self.lat = target_lat
            self.lon = target_lon

            if status == "ACCEPTED":
                # Arrived at pickup, notify arrived, then start ride (IN_PROGRESS)
                logger.info(f"[ARRIVED] Driver {self.name} has arrived at pickup for Ride #{ride_id}.")
                await async_http_post(f"/rides/{ride_id}/status", {"status": "ARRIVED"})
                
                await asyncio.sleep(2)  # Wait for rider to board
                logger.info(f"[IN_PROGRESS] Ride #{ride_id} started.")
                await async_http_post(f"/rides/{ride_id}/status", {"status": "IN_PROGRESS"})
                self.current_ride["status"] = "IN_PROGRESS"
            else:
                # Arrived at dropoff, complete ride
                logger.info(f"[COMPLETED] Ride #{ride_id} completed successfully!")
                await async_http_post(f"/rides/{ride_id}/status", {"status": "COMPLETED"})
                self.current_ride = None
                self.status = "AVAILABLE"
        else:
            # Take a step
            self.lat += (d_lat / distance_to_target) * speed
            self.lon += (d_lon / distance_to_target) * speed
            logger.info(f"[MOVING] Driver {self.name} is {step_desc}. Current: ({self.lat:.4f}, {self.lon:.4f})")


class RiderSimulator:
    def __init__(self, rider_id: str, name: str):
        self.rider_id = rider_id
        self.name = name

    async def run_ride_request(self, pickup: tuple, dropoff: tuple):
        # 1. Register rider with server
        try:
            await async_http_post("/users", {
                "id": self.rider_id,
                "name": self.name,
                "email": f"{self.rider_id}@example.com"
            })
            logger.info(f"Registered Rider {self.name}")
        except Exception:
            logger.info(f"Rider {self.rider_id} already exists.")

        # 2. Connect to Rider WebSocket to listen to events
        uri = f"{WS_URL}/ws/rider/{self.rider_id}"
        
        try:
            async with websockets.connect(uri) as websocket:
                logger.info(f"[{self.rider_id}] Connected to Rider WebSocket. Submitting request...")

                # Submit ride request
                ride = await async_http_post("/rides/request", {
                    "rider_id": self.rider_id,
                    "pickup_lat": pickup[0],
                    "pickup_lon": pickup[1],
                    "dropoff_lat": dropoff[0],
                    "dropoff_lon": dropoff[1]
                })
                logger.info(f"[REQUESTED] Ride requested! ID: {ride['id']}, Estimated Fare: ${ride['fare']}")

                # Listen for updates
                while True:
                    msg_str = await websocket.recv()
                    msg = json.loads(msg_str)
                    msg_type = msg.get("type")

                    if msg_type == "dispatch_update":
                        logger.info(f"[RIDER APP] Status update: {msg['status']}")

                    elif msg_type == "ride_accepted":
                        driver = msg["ride"]["driver_id"]
                        logger.info(f"[RIDER APP] Driver {driver} has ACCEPTED your ride request!")

                    elif msg_type == "ride_status_update":
                        status = msg["ride"]["status"]
                        logger.info(f"[RIDER APP] Ride status updated: {status}")
                        if status == "COMPLETED":
                            logger.info("[RIDER APP] Ride finished! Thank you for riding.")
                            break

                    elif msg_type == "driver_location_update":
                        # Log driver approaching coordinates
                        logger.info(f"[RIDER APP] Live Driver Coordinates: ({msg['lat']:.4f}, {msg['lon']:.4f})")

                    elif msg_type == "ride_failed":
                        logger.error(f"[RIDER APP] Match failed: {msg['reason']}")
                        break

        except Exception as e:
            logger.error(f"Error in rider simulation: {e}")

# Helper helper to calculate distance
import math

async def main():
    # Define some drivers starting around Bangalore city center
    drivers = [
        DriverSimulator("driver_1", "Bob (Closest)", 12.9715, 77.5915),
        DriverSimulator("driver_2", "Alice (Medium)", 12.9730, 77.5930),
        DriverSimulator("driver_3", "Charlie (Far)", 12.9820, 77.6010),
    ]

    # Spawn driver tasks
    driver_tasks = [asyncio.create_task(d.run()) for d in drivers]

    # Let drivers connect and register their first locations
    logger.info("Initializing simulated drivers and waiting 5s for location telemetry...")
    await asyncio.sleep(5)

    # Simulate rider request
    rider = RiderSimulator("rider_100", "Sachindra")
    pickup = (12.9700, 77.5900)
    dropoff = (12.9800, 77.6000)

    # Start the ride flow!
    await rider.run_ride_request(pickup, dropoff)

    # Keep running to let driver simulation finish
    await asyncio.sleep(10)
    
    # Cancel driver tasks
    for task in driver_tasks:
        task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Simulation stopped.")
