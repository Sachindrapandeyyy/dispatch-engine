import logging
import asyncio
from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import List, Dict

from app.database import Base, engine, get_db
from app.models import User, Driver, Ride
from app.config import settings
from app.geo_service import geo_service
from app.dispatch_service import connection_manager, dispatch_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("dispatch.main")

# Auto-create tables on startup (excellent for local development)
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Real-Time Geolocation Dispatch Engine API",
    description="A high-performance dispatch gateway mimicking Uber/DoorDash driver matching.",
    version="1.0.0"
)

# --- Pydantic Schemas ---

class UserCreate(BaseModel):
    id: str = Field(..., example="rider_123")
    name: str = Field(..., example="Alice Smith")
    email: str = Field(..., example="alice@example.com")

class DriverCreate(BaseModel):
    id: str = Field(..., example="driver_456")
    name: str = Field(..., example="Bob Jones")
    vehicle_type: str = Field("standard", example="premium")

class RideRequest(BaseModel):
    rider_id: str
    pickup_lat: float
    pickup_lon: float
    dropoff_lat: float
    dropoff_lon: float

class RideResponse(BaseModel):
    id: int
    rider_id: str
    driver_id: str | None
    status: str
    pickup_lat: float
    pickup_lon: float
    dropoff_lat: float
    dropoff_lon: float
    fare: float

class RideAction(BaseModel):
    driver_id: str

class StatusUpdate(BaseModel):
    status: str  # ARRIVED, IN_PROGRESS, COMPLETED, CANCELLED

# --- HTTP Endpoints ---

from fastapi.responses import HTMLResponse
import os

@app.get("/", response_class=HTMLResponse)
def read_index():
    static_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
    index_path = os.path.join(static_dir, "index.html")
    if not os.path.exists(index_path):
        return "<h3>Real-Time Dispatch Engine is running! Dashboard UI index.html not found.</h3>"
    with open(index_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/drivers/active")
def get_active_drivers(db: Session = Depends(get_db)):
    locations = geo_service.get_all_driver_locations()
    active_drivers = []
    
    for driver_id, coords in locations.items():
        status = geo_service.get_status(driver_id)
        db_driver = db.query(Driver).filter(Driver.id == driver_id).first()
        name = db_driver.name if db_driver else f"Driver {driver_id}"
        vehicle = db_driver.vehicle_type if db_driver else "standard"
        active_drivers.append({
            "id": driver_id,
            "name": name,
            "vehicle_type": vehicle,
            "status": status,
            "lat": coords[0],
            "lon": coords[1]
        })
    return active_drivers


@app.post("/users", response_model=UserCreate, status_code=201)
def create_user(user_data: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.id == user_data.id).first()
    if db_user:
        raise HTTPException(status_code=400, detail="User already exists")
    new_user = User(id=user_data.id, name=user_data.name, email=user_data.email)
    db.add(new_user)
    db.commit()
    return new_user

@app.post("/drivers", response_model=DriverCreate, status_code=201)
def create_driver(driver_data: DriverCreate, db: Session = Depends(get_db)):
    db_driver = db.query(Driver).filter(Driver.id == driver_data.id).first()
    if db_driver:
        raise HTTPException(status_code=400, detail="Driver already exists")
    new_driver = Driver(id=driver_data.id, name=driver_data.name, vehicle_type=driver_data.vehicle_type)
    db.add(new_driver)
    db.commit()
    # Register availability in geo_service
    geo_service.set_status(driver_data.id, "OFFLINE")
    return new_driver

@app.post("/rides/request", response_model=RideResponse, status_code=202)
async def request_ride(ride_req: RideRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    # 1. Verify User exists
    user = db.query(User).filter(User.id == ride_req.rider_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Rider (User) not found. Create user first.")

    # 2. Compute a dummy estimate fare based on distance (haversine)
    from app.geo_service import haversine
    dist = haversine(ride_req.pickup_lat, ride_req.pickup_lon, ride_req.dropoff_lat, ride_req.dropoff_lon)
    fare = round(max(5.0, dist * 2.5), 2)  # $2.50 per km, min $5.00

    # 3. Create Ride object in DB
    ride = Ride(
        rider_id=ride_req.rider_id,
        pickup_lat=ride_req.pickup_lat,
        pickup_lon=ride_req.pickup_lon,
        dropoff_lat=ride_req.dropoff_lat,
        dropoff_lon=ride_req.dropoff_lon,
        fare=fare,
        status="REQUESTED"
    )
    db.add(ride)
    db.commit()
    db.refresh(ride)

    logger.info(f"Ride request created. Ride ID: {ride.id}, Rider: {ride.rider_id}")
    
    # 4. Trigger Dispatch Loop in the background (FastAPI background tasks or asyncio task)
    # Using asyncio.create_task ensures the client gets a quick response, and the dispatch loop works in parallel.
    asyncio.create_task(dispatch_engine.dispatch_ride(ride.id, db))
    
    return ride.to_dict()

@app.post("/rides/{ride_id}/accept", status_code=200)
async def accept_ride(ride_id: int, action: RideAction, db: Session = Depends(get_db)):
    ride = db.query(Ride).filter(Ride.id == ride_id).first()
    if not ride:
        raise HTTPException(status_code=404, detail="Ride not found")
    
    if ride.status != "ASSIGNED" or ride.driver_id != action.driver_id:
        raise HTTPException(status_code=400, detail="Ride offer invalid, already assigned, or expired.")

    # Accept ride
    ride.status = "ACCEPTED"
    db.commit()

    # Clear dispatch exclusions for this ride
    dispatch_engine.clear_exclusions(ride_id)

    # Set driver status to BUSY in geospatial tracker
    geo_service.set_status(action.driver_id, "BUSY")

    # Notify rider via WebSocket
    await connection_manager.send_to_rider(
        rider_id=ride.rider_id,
        message={
            "type": "ride_accepted",
            "ride": ride.to_dict()
        }
    )

    logger.info(f"Ride {ride_id} accepted by Driver {action.driver_id}")
    return {"status": "success", "message": "Ride offer accepted."}

@app.post("/rides/{ride_id}/reject", status_code=200)
async def reject_ride(ride_id: int, action: RideAction, db: Session = Depends(get_db)):
    ride = db.query(Ride).filter(Ride.id == ride_id).first()
    if not ride:
        raise HTTPException(status_code=404, detail="Ride not found")
    
    if ride.status != "ASSIGNED" or ride.driver_id != action.driver_id:
        raise HTTPException(status_code=400, detail="No active offer for this driver.")

    logger.info(f"Ride {ride_id} rejected by Driver {action.driver_id}. Re-dispatching.")

    # Exclude driver
    dispatch_engine.exclude_driver(ride_id, action.driver_id)

    # Reset ride to REQUESTED
    ride.driver_id = None
    ride.status = "REQUESTED"
    db.commit()

    # Trigger next dispatch search immediately
    asyncio.create_task(dispatch_engine.dispatch_ride(ride_id, db))

    return {"status": "success", "message": "Ride offer rejected."}

@app.post("/rides/{ride_id}/status", status_code=200)
async def update_ride_status(ride_id: int, update: StatusUpdate, db: Session = Depends(get_db)):
    ride = db.query(Ride).filter(Ride.id == ride_id).first()
    if not ride:
        raise HTTPException(status_code=404, detail="Ride not found")

    valid_statuses = ["ARRIVED", "IN_PROGRESS", "COMPLETED", "CANCELLED"]
    if update.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Choose from: {valid_statuses}")

    ride.status = update.status
    db.commit()

    # If completed, free up the driver to AVAILABLE
    if update.status == "COMPLETED" and ride.driver_id:
        geo_service.set_status(ride.driver_id, "AVAILABLE")
        logger.info(f"Ride {ride_id} completed. Driver {ride.driver_id} is now AVAILABLE.")
    
    # Notify rider via WebSocket
    await connection_manager.send_to_rider(
        rider_id=ride.rider_id,
        message={
            "type": "ride_status_update",
            "ride": ride.to_dict()
        }
    )

    logger.info(f"Ride {ride_id} status updated to {update.status}")
    return {"status": "success", "new_status": update.status}

@app.get("/rides", response_model=List[RideResponse])
def list_rides(db: Session = Depends(get_db)):
    rides = db.query(Ride).order_by(Ride.id.desc()).all()
    return [r.to_dict() for r in rides]

# --- WebSockets Endpoints ---

@app.websocket("/ws/driver/{driver_id}")
async def websocket_driver_endpoint(websocket: WebSocket, driver_id: str, db: Session = Depends(get_db)):
    # 1. Verify driver exists in DB
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    if not driver:
        # Reject WS connection if driver not registered
        await websocket.close(code=4004)
        logger.warning(f"Rejected WebSocket connection from unregistered driver: {driver_id}")
        return

    await connection_manager.connect_driver(driver_id, websocket)
    
    # Mark driver as AVAILABLE on connect
    geo_service.set_status(driver_id, "AVAILABLE")

    try:
        while True:
            # Driver sends location telemetry: {"lat": 12.97, "lon": 77.59}
            data = await websocket.receive_json()
            if "lat" in data and "lon" in data:
                lat = float(data["lat"])
                lon = float(data["lon"])
                
                # Check current status
                status = geo_service.get_status(driver_id)
                # Keep active if busy, but update coordinates
                geo_service.update_location(driver_id, lat, lon)
                
                # Send back confirmation
                await websocket.send_json({
                    "type": "telemetry_ack",
                    "driver_id": driver_id,
                    "status": status,
                    "location": {"lat": lat, "lon": lon}
                })

                # Broadcast live location of driver to the active rider if ride in progress
                # Query rides in progress for this driver
                active_ride = db.query(Ride).filter(
                    Ride.driver_id == driver_id,
                    Ride.status.in_(["ACCEPTED", "ARRIVED", "IN_PROGRESS"])
                ).first()
                if active_ride:
                    await connection_manager.send_to_rider(
                        rider_id=active_ride.rider_id,
                        message={
                            "type": "driver_location_update",
                            "driver_id": driver_id,
                            "lat": lat,
                            "lon": lon
                        }
                    )
    except WebSocketDisconnect:
        connection_manager.disconnect_driver(driver_id)
        # On disconnect, set status to OFFLINE
        geo_service.set_status(driver_id, "OFFLINE")
    except Exception as e:
        logger.error(f"WebSocket error for driver {driver_id}: {e}")
        connection_manager.disconnect_driver(driver_id)
        geo_service.set_status(driver_id, "OFFLINE")


@app.websocket("/ws/rider/{rider_id}")
async def websocket_rider_endpoint(websocket: WebSocket, rider_id: str, db: Session = Depends(get_db)):
    # Verify user exists in DB
    user = db.query(User).filter(User.id == rider_id).first()
    if not user:
        await websocket.close(code=4004)
        logger.warning(f"Rejected WebSocket connection from unregistered rider: {rider_id}")
        return

    await connection_manager.connect_rider(rider_id, websocket)
    try:
        while True:
            # Rider doesn't need to push data constantly, keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        connection_manager.disconnect_rider(rider_id)
    except Exception as e:
        logger.error(f"WebSocket error for rider {rider_id}: {e}")
        connection_manager.disconnect_rider(rider_id)
