from core.autonomy import AutonomousEngine, resolve_bot_from_ctx
from core.swarm_control import swarm_controller


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
    data = intent.get("data", {})
    return await swarm_controller.play(ctx, data.get("query", ""), drone=data.get("drone"))

@router.register("pause")
async def pause(ctx,intent):
    return await swarm_controller.override(ctx, "PAUSE", drone=intent.get("data", {}).get("drone"))

@router.register("resume")
async def resume(ctx,intent):
    return await swarm_controller.override(ctx, "RESUME", drone=intent.get("data", {}).get("drone"))

@router.register("skip")
async def skip(ctx,intent):
    return await swarm_controller.override(ctx, "SKIP", drone=intent.get("data", {}).get("drone"))

@router.register("stop")
async def stop(ctx,intent):
    return await swarm_controller.override(ctx, "STOP", drone=intent.get("data", {}).get("drone"))

@router.register("leave")
async def leave(ctx,intent):
    return await swarm_controller.leave(ctx, drone=intent.get("data", {}).get("drone"))

@router.register("swarm_radar")
async def swarm_radar(ctx, intent):
    return await swarm_controller.radar(ctx)

@router.register("swarm_wrapped")
async def swarm_wrapped(ctx, intent):
    return await swarm_controller.wrapped(ctx)

@router.register("swarm_queue")
async def swarm_queue(ctx, intent):
    return await swarm_controller.queue_view(ctx, drone=intent.get("data", {}).get("drone"))

@router.register("swarm_shuffle")
async def swarm_shuffle(ctx, intent):
    return await swarm_controller.shuffle(ctx, drone=intent.get("data", {}).get("drone"))

@router.register("swarm_remove")
async def swarm_remove(ctx, intent):
    data = intent.get("data", {})
    return await swarm_controller.remove_track(ctx, data.get("drone"), data.get("track_number", 0))

@router.register("swarm_loop")
async def swarm_loop(ctx, intent):
    data = intent.get("data", {})
    return await swarm_controller.set_loop(ctx, data.get("mode", ""), drone=data.get("drone"))

@router.register("swarm_filter")
async def swarm_filter(ctx, intent):
    data = intent.get("data", {})
    return await swarm_controller.set_filter(ctx, data.get("filter_type", ""), drone=data.get("drone"))

@router.register("swarm_broadcast")
async def swarm_broadcast(ctx, intent):
    return await swarm_controller.broadcast(ctx, intent.get("data", {}).get("query", ""))

@router.register("swarm_set_home")
async def swarm_set_home(ctx, intent):
    data = intent.get("data", {})
    return await swarm_controller.set_home(ctx, data.get("drone"), data.get("channel_id"))

@router.register("swarm_undo")
async def swarm_undo(ctx, intent):
    return await swarm_controller.restore_backup(ctx, intent.get("data", {}).get("drone"))

@router.register("self_heal")
async def heal(ctx,intent):
    bot = resolve_bot_from_ctx(ctx)
    if bot is None:
        return "I couldn't resolve the bot context to run diagnostics."
    return await AutonomousEngine(bot).run_summary()
