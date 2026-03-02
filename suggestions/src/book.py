class Book:
    title: str
    author: str

    def __init__(self, title: str, author: str):
        self.title = title
        self.author = author

    def __str__(self) -> str:
        return "{title:" + f"{self.title}, author: {self.author}" + "}"
    
    def __repr__(self) -> str:
        return str(self)
