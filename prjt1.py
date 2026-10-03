import threading
import paho.mqtt.client as mqtt
from datetime import datetime
from fastapi import*
from pydantic import*
from sqlalchemy import*
from sqlalchemy.orm import*

#__________________broker setup____________________
broker = "broker.hivemq.com"
port = 1883
topic = "kilani/sensors/#"


#__________________data base setup____________
database_url = "sqlite:///./mydb.db"
engine = create_engine(database_url , connect_args={"check_same_thread" : False})
SessionLocal = sessionmaker(autocommit=False , autoflush=False , bind=engine)
Base = declarative_base()

#________________Model______________
class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    value = Column(Float)
    type = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
Base.metadata.create_all(bind=engine)

#____________shemas____________
class SensorRead(BaseModel):
    id: int
    value : float
    type : str
    timestamp : datetime
    class Config:
        from_attributes = True


#________________dependency________
def get_db():
    db=SessionLocal()
    try:
        yield db
    finally:
        db.close()

#________________recieve msg from mqtt______
def on_message(client, userdata, msg):
    db = SessionLocal()
    try:
        type = msg.topic.split("/")[-1]
        value = float(msg.payload.decode())
        reading = Sensor(type=type , value=value)
        db.add(reading)
        db.commit()
        print(f"recieved {type} : {value}")
    finally :
        db.close()


#___________setup mqtt subscriber____
def mqtt_subscriber():
    client = mqtt.Client()
    client.connect(broker, port, 60)
    client.subscribe(topic)
    client.on_message = on_message
    client.loop_forever()


#_______fastapi app_________
app = FastAPI()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=mqtt_subscriber)
    thread.daemon = True
    thread.start()


@app.get("/readings", response_model=list[SensorRead])
def get_readings(db: Session = Depends(get_db)):
    readings = db.query(Sensor).order_by(Sensor.timestamp.desc()).limit(10).all()
    return readings


@app.get("/readings/{sensor_type}", response_model=list[SensorRead])
def get_sensor_readings(sensor_type: str, db: Session = Depends(get_db)):
    readings = db.query(Sensor).filter(Sensor.type == sensor_type).order_by(Sensor.timestamp.desc()).limit(10).all()
    return readings



