from uuid import UUID
from pydantic import BaseModel, Field

class WarehouseStockResponse(BaseModel):
    warehouse_id: UUID = Field(..., description="ID склада")
    product_id: UUID = Field(..., description="ID товара")
    quantity: int = Field(..., description="Количество товара на складе")

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "warehouse_id": "e3ac683d-2fd5-42fe-9d09-d8d43dea47f6",
                "product_id": "40fa12dd-7c5f-43ea-a36e-f22024095cde",
                "quantity": 150
            }
        }
    }