from enum import Enum
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="title", description="description", version="0.1.0")


class Category(Enum):
    PLAYER = "player"
    OPTIONAL = "optional"
    GUEST = "guest"
    NONPLAYER = "non-player"


class Character(BaseModel):
    name: str
    age: int
    home: str
    id: int
    category: Category


characters = {
    0: Character(
        name="Cloud Strife", age=21, home="Nibelheim", id=0, category=Category.PLAYER
    ),
    1: Character(
        name="Tifa Lockhart", age=20, home="Nibelheim", id=1, category=Category.PLAYER
    ),
    2: Character(
        name="Aerith Gainsborough",
        age=22,
        home="Icicle Inn",
        id=2,
        category=Category.PLAYER,
    ),
    3: Character(
        name="Red XIII",
        age=48,
        home="Cosmo Canyon",
        id=3,
        category=Category.PLAYER,
    ),
    4: Character(
        name="Yuffie Kisaragi",
        age=16,
        home="Wutai",
        id=4,
        category=Category.OPTIONAL,
    ),
    5: Character(
        name="Sephiroth",
        age=25,
        home="Nibelheim",
        id=5,
        category=Category.GUEST,
    ),
    6: Character(
        name="Scarlet",
        age=40,
        home="Midgar",
        id=6,
        category=Category.NONPLAYER,
    ),
}


@app.get("/")
def index() -> dict[str, dict[int, Character]]:
    return {"characters": characters}


@app.get("/characters/{character_id}")
def query_character_by_id(character_id: int) -> Character:
    if character_id not in characters:
        raise HTTPException(
            status_code=404, detail=f"Character with {character_id=} does not exist."
        )
    return characters[character_id]


# dictionary containing the user's query arguments
Selection = dict[str, str | int | str | Category | None]


@app.get("/characters/")
def query_character_by_parameter(
    name: str | None = None,
    age: int | None = None,
    home: str | None = None,
    category: Category | None = None,
) -> dict[str, Selection | list[Character]]:

    def check_character(character: Character) -> bool:
        return all(
            (
                name is None or character.name == name,
                age is None or character.age == age,
                home is None or character.home == home,
                category is None or character.category == category,
            )
        )

    selection = [
        character for character in characters.values() if check_character(character)
    ]
    return {
        "query": {"name": name, "age": age, "home": home, "category": category},
        "selection": selection,
    }


#
from pathlib import Path
from fastapi.responses import FileResponse


@app.get("/get_image")
async def get_image():
    image_path = Path("gfglogo.jpg")
    if not image_path.is_file():
        return {"error": "Image not found on the server"}
    return FileResponse(image_path)
