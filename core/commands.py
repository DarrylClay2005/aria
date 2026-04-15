class Router:
    def __init__(self):
        self.cmds={}
    def register(self,name):
        def wrap(f):
            self.cmds[name]=f
            return f
        return wrap
    async def execute(self,name,ctx,intent):
        if name not in self.cmds:
            return None
        return await self.cmds[name](ctx,intent)

router=Router()

@router.register("play")
async def play(ctx,intent):
    return f"Playing {intent.get('data',{}).get('query')}"

@router.register("pause")
async def pause(ctx,intent):
    return "Paused"

@router.register("skip")
async def skip(ctx,intent):
    return "Skipped"

@router.register("stop")
async def stop(ctx,intent):
    return "Stopped"

@router.register("self_heal")
async def heal(ctx,intent):
    return "Running diagnostics..."
