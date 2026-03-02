class Book:
    title: str
    author: str
    id: str

    def __init__(self, title: str, author: str, id: str):
        self.title = title
        self.author = author
        self.id = id

    def __str__(self) -> str:
        return "{title:" + f"{self.title}, author: {self.author}, isbn: {self.id}" + "}"
    
    def __repr__(self) -> str:
        return str(self)
