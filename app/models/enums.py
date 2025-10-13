from enum import Enum

# Событие перемещения
class MovementEvent(str, Enum):
    arrival = "arrival"
    departure = "departure"

# Тип сообщения для Kafka
class EventType(str, Enum):
    WAREHOUSE_MOVEMENT = "ru.retail.warehouses.movement"

# Версия спецификации
class SpecVersion(str, Enum):
    V1_0 = "1.0"

# Тип содержимого
class DataContentType(str, Enum):
    JSON = "application/json"