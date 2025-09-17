"""
Realtime user document generator (minimal)
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from faker import Faker


class UserDataGenerator:
    def __init__(self, num_user: int, num_app: int):
        self.fake = Faker(['en_US'])
        self.num_user = num_user
        # Pre-generate app ids [app_001 .. app_XXX]
        safe_num_app = max(1, int(num_app))
        self.app_ids: List[str] = [f"app_{i:03d}" for i in range(1, safe_num_app + 1)]
        # Pre-generate user uids from 1 to num_user
        self.user_uids: List[str] = [f"user_{i}" for i in range(1, num_user + 1)]
        # Initialize per-user level state (starting from a random base like before)
        self._user_levels: Dict[str, int] = {
            uid: 0 for uid in self.user_uids
        }
        # Pre-initialize fixed user data to keep constant fields stable across generations
        # Fixed fields: _id, uid, appId, avatar, role, name, devices, email, authProvider, resources
        self._fixed_users: Dict[str, Dict[str, Any]] = {}
        for uid in self.user_uids:
            fb_inst_id = self.fake.pystr(min_chars=22, max_chars=22)
            self._fixed_users[uid] = {
                "_id": str(uuid.uuid4()),
                "uid": uid,
                "geo": self.fake.random_element(elements=("US", "UK", "DE", "FR", "JP", "VN")),
                "avatar": str(self.fake.random_int(min=1, max=20)),
                "role": str(uuid.uuid4()),
                "name": f"Player {self.fake.random_number(digits=8, fix_len=True)}",
                "devices": [
                    {
                        "fb_analytics_instance_id": self.fake.pystr(min_chars=32, max_chars=32).upper(),
                        "fb_instance_id": fb_inst_id,
                        "fcmToken": "empty",
                    }
                ],
                # Optional constant placeholders (stay constant unless you change initializer):
                "email": None,
                "authProvider": None,
                "resources": [],
            }

    def generate_user_submission(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        if now is None:
            now = datetime.now(timezone.utc)
        # Select a user uid from the pre-generated list
        uid = random.choice(self.user_uids)
        # Dynamic fields only below (fixed are loaded from cache)
        
        
        weights = [10 - i for i in range(10)]  # [10,9,8,...,1]
        increase = random.choices(range(1, 11), weights=weights, k=1)[0]
        self._user_levels[uid] += increase
        
        # Build ISO strings with milliseconds and Z suffix
        iso_ms = now.astimezone(timezone.utc).isoformat(timespec='milliseconds')
        iso_z = iso_ms.replace('+00:00', 'Z')
        last_login_iso_date = iso_z
        created_iso_date = iso_z
        updated_iso_date = iso_z

        base = self._fixed_users[uid]
        # Compose document with explicit key order
        document = {
            "_id": base["_id"],
            "uid": base["uid"],
            "email": base["email"],
            "authProvider": base["authProvider"],
            "appId": random.choice(self.app_ids),
            "avatar": base["avatar"],
            "geo": base["geo"],
            "role": base["role"],
            "lastLoginAt": last_login_iso_date,
            "name": base["name"],
            "devices": base["devices"],
            "resources": base["resources"],
            "created_at": created_iso_date,
            "updated_at": updated_iso_date,
            "level": self._user_levels[uid],
            "updatedAt": updated_iso_date,
            "team": self.fake.random_int(min=1, max=10),
        }
        return document

# ----------------------------------------------------------
# Example emitted document (field order preserved):
# {
#   "_id": "string (uuid)",
#   "uid": "user_x",
#   "email": null,
#   "authProvider": null,
#   "appId": "app_001 ... app_XXX",            # chosen randomly per emission
#   "avatar": "1..20 (string)",
#   "geo": "US|UK|DE|FR|JP|VN",                 # fixed per uid
#   "role": "string (uuid)",                    # fixed per uid
#   "lastLoginAt": "YYYY-MM-DDTHH:MM:SS.mmmZ",
#   "name": "Player 12345678",                  # fixed per uid
#   "devices": [                                 # fixed per uid
#     {
#       "fb_analytics_instance_id": "32 uppercase chars",
#       "fb_instance_id": "22 chars",
#       "fcmToken": "empty"
#     }
#   ],
#   "resources": [],
#   "created_at": "YYYY-MM-DDTHH:MM:SS.mmmZ",
#   "updated_at": "YYYY-MM-DDTHH:MM:SS.mmmZ",
#   "level": 1..300,
#   "updatedAt": "YYYY-MM-DDTHH:MM:SS.mmmZ",
#   "team": 1..10
# }
# ----------------------------------------------------------