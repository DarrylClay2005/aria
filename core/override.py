class OverrideManager:
    def __init__(self):
        self.override_users=set()
        self.autonomy_enabled=True

    def enable_override(self,user_id):
        self.override_users.add(user_id)

    def can_override(self,user_id):
        return user_id in self.override_users

    def toggle(self,state):
        self.autonomy_enabled=state

override_manager=OverrideManager()
