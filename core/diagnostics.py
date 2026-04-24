import datetime

class DiagnosticsEngine:
    def analyze_error(self,e):
        msg=str(e).lower()
        fix="Unknown"
        if "voice" in msg:
            fix="Reconnect VC"
        elif "timeout" in msg:
            fix="Retry request"
        return {
            "time":str(datetime.datetime.utcnow()),
            "error":str(e),
            "fix":fix
        }
