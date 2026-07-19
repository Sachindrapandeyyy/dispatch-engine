import datetime
from sqlalchemy import Column, String, Float, DateTime, ForeignKey, Integer
from sqlalchemy.orm import relationship
from app.database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    rides = relationship("Ride", back_populates="rider")


class Driver(Base):
    __tablename__ = "drivers"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    vehicle_type = Column(String, default="standard")  # standard, premium, xl
    # Active state in database (offline vs online is managed dynamically in Redis cache)
    is_active = Column(Integer, default=1)  # 1 for active/registered, 0 for deactivated
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    rides = relationship("Ride", back_populates="driver")


class Ride(Base):
    __tablename__ = "rides"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    rider_id = Column(String, ForeignKey("users.id"), nullable=False)
    driver_id = Column(String, ForeignKey("drivers.id"), nullable=True)
    
    # Ride statuses: REQUESTED -> ASSIGNED -> ACCEPTED -> IN_PROGRESS -> COMPLETED | CANCELLED
    status = Column(String, default="REQUESTED", index=True)
    
    pickup_lat = Column(Float, nullable=False)
    pickup_lon = Column(Float, nullable=False)
    dropoff_lat = Column(Float, nullable=False)
    dropoff_lon = Column(Float, nullable=False)
    
    fare = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    # Relationships
    rider = relationship("User", back_populates="rides")
    driver = relationship("Driver", back_populates="rides")

    def to_dict(self):
        return {
            "id": self.id,
            "rider_id": self.rider_id,
            "driver_id": self.driver_id,
            "status": self.status,
            "pickup_lat": self.pickup_lat,
            "pickup_lon": self.pickup_lon,
            "dropoff_lat": self.dropoff_lat,
            "dropoff_lon": self.dropoff_lon,
            "fare": self.fare,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
