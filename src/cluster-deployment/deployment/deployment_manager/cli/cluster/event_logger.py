import json
import os
from collections import Counter, defaultdict
from datetime import datetime


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


class UpgradeEventLogger:
    """
    Logger for upgrade events. Instantiate, then call set_upgrade_id(upgrade_id) before using other methods.
    """

    def __init__(self):
        """
        Initialize the UpgradeEventLogger.
        Important: You must call set_upgrade_id(upgrade_id) before using upgrade/step methods.
        """
        self.upgrade_id = None
        self.log_dir = os.environ["DEPLOYMENT_LOGS"]
        self.file_path = None
        os.makedirs(self.log_dir, exist_ok=True)

    def set_upgrade_id(self, db_update_only, upgrade_id):
        if db_update_only:
            return
        self.upgrade_id = upgrade_id
        self.file_path = os.path.join(self.log_dir, f"upgrade_{upgrade_id}.json")
        # Initialize file if it doesn't exist
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w") as f:
                json.dump(
                    {
                        "upgrade_id": upgrade_id,
                        "start_time": None,
                        "end_time": None,
                        "steps": []
                    }, f, indent=2
                )

    def _require_upgrade_id(self):
        if not self.upgrade_id or not self.file_path:
            raise RuntimeError("Upgrade ID not set. Call set_upgrade_id(upgrade_id) before using this method.")

    def _load(self):
        self._require_upgrade_id()
        with open(self.file_path) as f:
            return json.load(f)

    def _save(self, data):
        self._require_upgrade_id()
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2)

    def upgrade_start(self, db_update_only):
        if db_update_only:
            return
        data = self._load()
        data["start_time"] = now_iso()
        self._save(data)

    def upgrade_end(self, db_update_only):
        if db_update_only:
            return
        data = self._load()
        data["end_time"] = now_iso()
        self._save(data)

    def step_start(self, db_update_only, step_name):
        if db_update_only:
            return
        data = self._load()
        step = {
            "step_name": step_name,
            "start_time": now_iso(),
            "end_time": None,
            "result": None,
            "error_message": None
        }
        data["steps"].append(step)
        self._save(data)

    def step_end(self, db_update_only, step_name, is_successful, error_message=None):
        if db_update_only:
            return
        data = self._load()
        # Find the last step with this name (not yet ended)
        for step in reversed(data["steps"]):
            if step["step_name"] == step_name and step["end_time"] is None:
                step["end_time"] = now_iso()
                step["result"] = "success" if is_successful == True else "failure"
                if error_message:
                    step["error_message"] = error_message
                break
        self._save(data)

    @staticmethod
    def analyze_upgrade_log(log_path):
        """
        Analyze the upgrade log at log_path and print summary statistics.
        """

        def parse_time(t):
            # Expects ISO8601 with Z
            if t is None:
                return None
            try:
                # Remove Z if present
                if t.endswith("Z"):
                    t = t[:-1]
                return datetime.fromisoformat(t)
            except Exception:
                return None

        with open(log_path) as f:
            log = json.load(f)

        upgrade_id = log.get("upgrade_id")
        start_time = log.get("start_time")
        end_time = log.get("end_time")
        steps = log.get("steps", [])

        start_dt = parse_time(start_time)
        end_dt = parse_time(end_time)
        duration = None
        if start_dt and end_dt:
            duration = (end_dt - start_dt).total_seconds()

        print(f"Upgrade ID: {upgrade_id}")
        print(f"Start time: {start_time}")
        print(f"End time:   {end_time}")
        if duration is not None:
            print(f"Duration:   {duration:.1f} seconds")
        else:
            print("Duration:   N/A")

        total_steps = len(steps)
        succeeded = sum(1 for s in steps if s.get("result") == "success")
        failed = sum(1 for s in steps if s.get("result") in ("failure", "error"))

        print(f"Total Steps: {total_steps} (Succeeded: {succeeded}, Failed: {failed})")

        # Group by step_name
        step_stats = defaultdict(list)
        for s in steps:
            step_stats[s.get("step_name")].append(s)

        print("\nStep breakdown:")
        for step_name, step_list in step_stats.items():
            count = len(step_list)
            durations = []
            failures = 0
            error_msgs = []
            for s in step_list:
                st = parse_time(s.get("start_time"))
                et = parse_time(s.get("end_time"))
                if st and et and s.get("result") == "success":
                    durations.append((et - st).total_seconds())
                if s.get("result") in ("failure", "error"):
                    failures += 1
                    if s.get("error_message"):
                        error_msgs.append(s.get("error_message"))
            print(f"  - {step_name}:")
            print(f"      Ran: {count} times")
            if durations:
                print(f"      Success durations: {[f'{d:.2f}' for d in durations]} seconds")
            else:
                print(f"      Success durations: N/A")
            print(f"      Failures: {failures}")
            if failures > 0 and error_msgs:
                top_reasons = Counter(error_msgs).most_common(3)
                print(f"      Top failure reasons:")
                for msg, cnt in top_reasons:
                    print(f"          ({cnt}x) {msg}")
