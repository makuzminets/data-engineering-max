"""
FastAPI service for real-time fraud scoring.
Provides risk assessment for new signups.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Fraud Detection API",
    description="Real-time signup fraud scoring using ML models",
    version="1.0.0"
)

# Global model cache
MODEL_CACHE = {}


class SignupRequest(BaseModel):
    """Signup data for fraud scoring."""
    
    user_id: str = Field(..., description="Unique user identifier")
    email: str = Field(..., description="User email address")
    ip_address: str = Field(..., description="User IP address")
    user_agent: str = Field(default="", description="Browser user agent")
    
    # Session behavior
    session_duration_seconds: int = Field(default=60, ge=0)
    pages_viewed: int = Field(default=2, ge=0)
    form_fill_time_seconds: int = Field(default=60, ge=0)
    mouse_movements: int = Field(default=100, ge=0)
    typing_speed_cpm: int = Field(default=200, ge=0, description="Characters per minute")
    
    # Device info
    screen_resolution: str = Field(default="1920x1080")
    timezone_offset: int = Field(default=0)
    is_vpn: bool = Field(default=False)
    
    # Referral
    referral_source: str = Field(default="direct")
    
    # Profile
    profile_completeness: float = Field(default=0.5, ge=0, le=1)
    has_profile_photo: bool = Field(default=False)
    bio_length: int = Field(default=0, ge=0)
    
    # Geographic
    claimed_country: str = Field(default="US")
    ip_country: str = Field(default="US")
    
    # Velocity (from lookup)
    ip_signup_count_24h: int = Field(default=0, ge=0)
    device_signup_count_24h: int = Field(default=0, ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_12345",
                "email": "john.smith42@gmail.com",
                "ip_address": "192.168.1.1",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                "session_duration_seconds": 180,
                "pages_viewed": 5,
                "form_fill_time_seconds": 90,
                "mouse_movements": 350,
                "typing_speed_cpm": 250,
                "screen_resolution": "1920x1080",
                "timezone_offset": -5,
                "is_vpn": False,
                "referral_source": "google",
                "profile_completeness": 0.7,
                "has_profile_photo": True,
                "bio_length": 150,
                "claimed_country": "US",
                "ip_country": "US",
                "ip_signup_count_24h": 1,
                "device_signup_count_24h": 0
            }
        }


class FraudScoreResponse(BaseModel):
    """Fraud scoring response."""
    
    user_id: str
    fraud_probability: float = Field(..., description="Probability of fraud (0-1)")
    risk_level: str = Field(..., description="Risk level: low, medium, high, critical")
    risk_score: float = Field(..., description="Composite risk score")
    decision: str = Field(..., description="Recommended action: approve, review, block")
    
    # Risk breakdown
    email_risk: float
    behavior_risk: float
    device_risk: float
    geo_risk: float
    velocity_risk: float
    
    # Flags
    risk_flags: List[str] = Field(default_factory=list)
    
    # Metadata
    model_version: str
    scored_at: datetime


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    model_loaded: bool
    model_version: str


def load_model():
    """Load the trained model."""
    if 'model' in MODEL_CACHE:
        return MODEL_CACHE['model']
    
    model_dir = Path(__file__).parent.parent / "models" / "saved_models"
    
    # Find latest model
    model_files = list(model_dir.glob("xgboost_*.pkl"))
    if not model_files:
        model_files = list(model_dir.glob("lightgbm_*.pkl"))
    
    if not model_files:
        logger.warning("No trained model found. Using mock model.")
        MODEL_CACHE['model'] = None
        MODEL_CACHE['version'] = "mock"
        return None
    
    latest_model = max(model_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Loading model from {latest_model}")
    
    with open(latest_model, 'rb') as f:
        model_data = pickle.load(f)
    
    MODEL_CACHE['model'] = model_data['model']
    MODEL_CACHE['feature_columns'] = model_data['feature_columns']
    MODEL_CACHE['feature_engineer'] = model_data['feature_engineer']
    MODEL_CACHE['version'] = latest_model.stem
    
    logger.info(f"Model loaded: {MODEL_CACHE['version']}")
    return MODEL_CACHE['model']


def prepare_features(request: SignupRequest) -> pd.DataFrame:
    """Convert request to feature DataFrame."""
    
    data = {
        'user_id': [request.user_id],
        'signup_timestamp': [datetime.now()],
        'email': [request.email],
        'ip_address': [request.ip_address],
        'user_agent': [request.user_agent],
        'session_duration_seconds': [request.session_duration_seconds],
        'pages_viewed': [request.pages_viewed],
        'form_fill_time_seconds': [request.form_fill_time_seconds],
        'mouse_movements': [request.mouse_movements],
        'typing_speed_cpm': [request.typing_speed_cpm],
        'screen_resolution': [request.screen_resolution],
        'timezone_offset': [request.timezone_offset],
        'is_vpn': [request.is_vpn],
        'referral_source': [request.referral_source],
        'profile_completeness': [request.profile_completeness],
        'has_profile_photo': [request.has_profile_photo],
        'bio_length': [request.bio_length],
        'email_has_numbers': [any(c.isdigit() for c in request.email.split('@')[0])],
        'email_length': [len(request.email.split('@')[0])],
        'claimed_country': [request.claimed_country],
        'ip_country': [request.ip_country],
        'country_mismatch': [request.claimed_country != request.ip_country],
        'ip_signup_count_24h': [request.ip_signup_count_24h],
        'device_signup_count_24h': [request.device_signup_count_24h],
    }
    
    return pd.DataFrame(data)


def get_risk_level(probability: float) -> str:
    """Convert probability to risk level."""
    if probability < 0.2:
        return "low"
    elif probability < 0.5:
        return "medium"
    elif probability < 0.8:
        return "high"
    else:
        return "critical"


def get_decision(probability: float, risk_level: str) -> str:
    """Get recommended action based on risk."""
    if risk_level == "low":
        return "approve"
    elif risk_level == "medium":
        return "approve"  # Low-friction, monitor later
    elif risk_level == "high":
        return "review"
    else:
        return "block"


def get_risk_flags(request: SignupRequest, features_df: pd.DataFrame) -> List[str]:
    """Identify specific risk flags."""
    flags = []
    
    # Email flags
    disposable_domains = {"tempmail.com", "guerrillamail.com", "10minutemail.com", "mailinator.com", "yopmail.com"}
    email_domain = request.email.split('@')[1]
    if email_domain in disposable_domains:
        flags.append("disposable_email")
    
    # Behavioral flags
    if request.form_fill_time_seconds < 10:
        flags.append("suspicious_form_speed")
    if request.typing_speed_cpm > 600:
        flags.append("inhuman_typing_speed")
    if request.session_duration_seconds < 30:
        flags.append("very_short_session")
    
    # Device flags
    if request.is_vpn:
        flags.append("vpn_detected")
    if not request.user_agent or 'bot' in request.user_agent.lower():
        flags.append("suspicious_user_agent")
    
    # Geographic flags
    if request.claimed_country != request.ip_country:
        flags.append("country_mismatch")
    
    # Velocity flags
    if request.ip_signup_count_24h > 3:
        flags.append("high_ip_velocity")
    if request.device_signup_count_24h > 2:
        flags.append("high_device_velocity")
    
    return flags


@app.on_event("startup")
async def startup_event():
    """Load model on startup."""
    load_model()


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    model = load_model()
    return HealthResponse(
        status="healthy",
        model_loaded=model is not None,
        model_version=MODEL_CACHE.get('version', 'none')
    )


@app.post("/score", response_model=FraudScoreResponse)
async def score_signup(request: SignupRequest):
    """
    Score a signup for fraud risk.
    
    Returns fraud probability, risk level, and recommended action.
    """
    
    model = load_model()
    
    # Prepare features
    raw_df = prepare_features(request)
    
    if model is not None and 'feature_engineer' in MODEL_CACHE:
        # Use trained model
        feature_engineer = MODEL_CACHE['feature_engineer']
        features_df = feature_engineer.transform(raw_df)
        
        feature_columns = MODEL_CACHE['feature_columns']
        X = features_df[feature_columns]
        
        # Get prediction
        fraud_probability = float(model.predict_proba(X)[0, 1])
    else:
        # Mock scoring (rule-based fallback)
        features_df = raw_df
        fraud_probability = calculate_mock_score(request)
    
    # Calculate risk components
    email_risk = 0.0
    if 'tempmail' in request.email or 'mailinator' in request.email:
        email_risk = 0.8
    
    behavior_risk = 0.0
    if request.form_fill_time_seconds < 10:
        behavior_risk += 0.3
    if request.typing_speed_cpm > 600:
        behavior_risk += 0.4
    
    device_risk = 0.3 if request.is_vpn else 0.0
    geo_risk = 0.4 if request.claimed_country != request.ip_country else 0.0
    velocity_risk = min(1.0, request.ip_signup_count_24h * 0.2 + request.device_signup_count_24h * 0.3)
    
    # Get risk level and decision
    risk_level = get_risk_level(fraud_probability)
    decision = get_decision(fraud_probability, risk_level)
    risk_flags = get_risk_flags(request, features_df)
    
    return FraudScoreResponse(
        user_id=request.user_id,
        fraud_probability=round(fraud_probability, 4),
        risk_level=risk_level,
        risk_score=round(fraud_probability * 100, 2),
        decision=decision,
        email_risk=round(email_risk, 2),
        behavior_risk=round(behavior_risk, 2),
        device_risk=round(device_risk, 2),
        geo_risk=round(geo_risk, 2),
        velocity_risk=round(velocity_risk, 2),
        risk_flags=risk_flags,
        model_version=MODEL_CACHE.get('version', 'mock'),
        scored_at=datetime.now()
    )


@app.post("/batch_score")
async def batch_score(requests: List[SignupRequest]):
    """Score multiple signups in batch."""
    results = []
    for req in requests:
        result = await score_signup(req)
        results.append(result)
    return results


def calculate_mock_score(request: SignupRequest) -> float:
    """Rule-based fallback scoring when model is not available."""
    score = 0.0
    
    # Email checks
    email_domain = request.email.split('@')[1]
    if email_domain in {"tempmail.com", "guerrillamail.com", "10minutemail.com", "mailinator.com"}:
        score += 0.3
    
    # Behavioral checks
    if request.form_fill_time_seconds < 10:
        score += 0.2
    if request.typing_speed_cpm > 600:
        score += 0.2
    if request.session_duration_seconds < 30:
        score += 0.1
    
    # Device checks
    if request.is_vpn:
        score += 0.15
    if not request.user_agent:
        score += 0.1
    
    # Velocity checks
    if request.ip_signup_count_24h > 3:
        score += 0.2
    if request.device_signup_count_24h > 2:
        score += 0.15
    
    # Geographic checks
    if request.claimed_country != request.ip_country:
        score += 0.15
    
    return min(1.0, score)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
