class IntentParser:
    async def parse(self,message:str):
        m=message.lower()
        if "play" in m:
            return {"action":"play","data":{"query":message}}
        if "pause" in m:
            return {"action":"pause"}
        if "skip" in m:
            return {"action":"skip"}
        if "stop" in m:
            return {"action":"stop"}
        if "fix" in m:
            return {"action":"self_heal"}
        return {"action":"unknown"}
