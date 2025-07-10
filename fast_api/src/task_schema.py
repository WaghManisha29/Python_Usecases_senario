from pydantic import BaseModel

class TaskBase(BaseModel):
    title: str
    description: str
    completed: bool = False

class TaskCreate(TaskBase):
    pass

class TaskUpdate(TaskBase):
    pass

class TaskOut(TaskBase):
    id: int

    class Config:
        from_attributes = True  # for Pydantic v2
