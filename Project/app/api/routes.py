import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


from fastapi import APIRouter
from app.services import index

router = APIRouter()

@router.post("/build-index")
def build(start_date: str, end_date: str = None):
    return index.build_index(start_date, end_date)

@router.get("/index-performance")
def performance(start_date: str, end_date: str):
    return index.get_performance(start_date, end_date)

@router.get("/index-composition")
def composition(date: str):
    return index.get_composition(date)

@router.get("/composition-changes")
def composition_changes(start_date: str, end_date: str):
    return index.get_composition_changes(start_date, end_date)

@router.post("/export-data")
def export(start_date: str, end_date: str):
    return index.export_to_excel(start_date, end_date)
