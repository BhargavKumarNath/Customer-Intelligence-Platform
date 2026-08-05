from __future__ import annotations

from functools import lru_cache

from src.config import get_settings
from src.db import get_connection_manager
from src.services.experiments import ABTestService
from src.services.propensity import PropensityService, load_model
from src.services.recommendations import RecommendationService
from src.services.segmentation import SegmentationService


@lru_cache(maxsize=1)
def get_segmentation_service() -> SegmentationService:
    return SegmentationService(get_connection_manager())


@lru_cache(maxsize=1)
def get_recommendation_service() -> RecommendationService:
    return RecommendationService(get_connection_manager())


@lru_cache(maxsize=1)
def get_ab_test_service() -> ABTestService:
    return ABTestService(get_connection_manager())


@lru_cache(maxsize=1)
def get_propensity_service() -> PropensityService:
    settings = get_settings()
    model = load_model(settings.model_path)
    return PropensityService(get_connection_manager(), model)
