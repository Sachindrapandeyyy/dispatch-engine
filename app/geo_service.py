import time
import logging
import math
from typing import List, Tuple, Dict
import redis
from app.config import settings

logger = logging.getLogger("dispatch.geo")

# Haversine distance calculator for fallback memory mode
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0  # Earth's radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

class InMemoryGeoService:
    """Fallback geospatial manager if Redis is unavailable."""
    def __init__(self):
        # schema: {driver_id: {"lat": float, "lon": float, "timestamp": float}}
        self.locations: Dict[str, dict] = {}
        # schema: {driver_id: status_str}
        self.statuses: Dict[str, str] = {}
        logger.info("Initialized In-Memory Geolocation Service fallback.")

    def update_location(self, driver_id: str, lat: float, lon: float) -> None:
        self.locations[driver_id] = {
            "lat": lat,
            "lon": lon,
            "timestamp": time.time()
        }
        if driver_id not in self.statuses:
            self.statuses[driver_id] = "AVAILABLE"

    def set_status(self, driver_id: str, status: str) -> None:
        self.statuses[driver_id] = status

    def get_status(self, driver_id: str) -> str:
        return self.statuses.get(driver_id, "OFFLINE")

    def get_nearby_drivers(self, lat: float, lon: float, radius_km: float) -> List[Tuple[str, float]]:
        nearby = []
        now = time.time()
        expiry = settings.DRIVER_HEARTBEAT_TIMEOUT_SEC

        for driver_id, loc in list(self.locations.items()):
            # Check heartbeat expiry
            if now - loc["timestamp"] > expiry:
                self.statuses[driver_id] = "OFFLINE"
                continue
            
            # Only search for available drivers
            if self.statuses.get(driver_id) != "AVAILABLE":
                continue

            dist = haversine(lat, lon, loc["lat"], loc["lon"])
            if dist <= radius_km:
                nearby.append((driver_id, dist))
        
        # Sort by distance (closest first)
        nearby.sort(key=lambda x: x[1])
        return nearby

    def get_all_driver_locations(self) -> Dict[str, Tuple[float, float]]:
        locations = {}
        now = time.time()
        expiry = settings.DRIVER_HEARTBEAT_TIMEOUT_SEC
        for driver_id, loc in list(self.locations.items()):
            if now - loc["timestamp"] <= expiry and self.statuses.get(driver_id) != "OFFLINE":
                locations[driver_id] = (loc["lat"], loc["lon"])
        return locations



class RedisGeoService:
    """Production-grade geospatial manager using Redis Geospatial commands."""
    def __init__(self, r_client: redis.Redis):
        self.r = r_client
        self.geo_key = "driver_locations"
        logger.info("Initialized Redis Geolocation Service.")

    def _status_key(self, driver_id: str) -> str:
        return f"driver:{driver_id}:status"

    def update_location(self, driver_id: str, lat: float, lon: float) -> None:
        # Add to Redis Geospatial Index (GEOADD key longitude latitude member)
        self.r.geoadd(self.geo_key, (lon, lat, driver_id))
        
        # Set status to AVAILABLE if not set or if offline
        s_key = self._status_key(driver_id)
        current_status = self.r.get(s_key)
        if not current_status or current_status.decode() == "OFFLINE":
            self.r.set(s_key, "AVAILABLE", ex=settings.DRIVER_HEARTBEAT_TIMEOUT_SEC)
        else:
            # Refresh expiry on active status
            self.r.expire(s_key, settings.DRIVER_HEARTBEAT_TIMEOUT_SEC)

    def set_status(self, driver_id: str, status: str) -> None:
        s_key = self._status_key(driver_id)
        if status == "OFFLINE":
            self.r.delete(s_key)
            self.r.zrem(self.geo_key, driver_id)
        else:
            self.r.set(s_key, status, ex=settings.DRIVER_HEARTBEAT_TIMEOUT_SEC)

    def get_status(self, driver_id: str) -> str:
        s_key = self._status_key(driver_id)
        val = self.r.get(s_key)
        return val.decode() if val else "OFFLINE"

    def get_nearby_drivers(self, lat: float, lon: float, radius_km: float) -> List[Tuple[str, float]]:
        # Redis GEOSEARCH: search for members within a radius
        # Returns list of tuples: (member_name, distance_from_center)
        try:
            results = self.r.geosearch(
                name=self.geo_key,
                longitude=lon,
                latitude=lat,
                radius=radius_km,
                unit="km",
                withdist=True,
                sort="ASC"
            )
        except Exception as e:
            logger.error(f"Error in geosearch: {e}")
            return []

        nearby = []
        for res in results:
            driver_id = res[0].decode() if isinstance(res[0], bytes) else res[0]
            dist = res[1]
            
            # Verify driver is currently AVAILABLE (not busy or offline)
            if self.get_status(driver_id) == "AVAILABLE":
                nearby.append((driver_id, dist))
        
        return nearby

    def get_all_driver_locations(self) -> Dict[str, Tuple[float, float]]:
        locations = {}
        try:
            # Get all driver members in the geo index
            driver_ids = [d.decode() if isinstance(d, bytes) else d for d in self.r.zrange(self.geo_key, 0, -1)]
            if not driver_ids:
                return {}
            # Fetch coordinates
            positions = self.r.geopos(self.geo_key, *driver_ids)
            for d_id, pos in zip(driver_ids, positions):
                if pos and self.get_status(d_id) != "OFFLINE":
                    # pos is (longitude, latitude)
                    locations[d_id] = (pos[1], pos[0])
        except Exception as e:
            logger.error(f"Error getting all driver locations from Redis: {e}")
        return locations



# Auto-configure service instantiation
try:
    r_conn = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        socket_connect_timeout=1.0,
        socket_timeout=1.0
    )
    # Ping test
    r_conn.ping()
    geo_service = RedisGeoService(r_conn)
except Exception as e:
    logger.warning(f"Could not connect to Redis: {e}. Defaulting to In-Memory Geo Service.")
    geo_service = InMemoryGeoService()

