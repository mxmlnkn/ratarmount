import asyncio

import asyncssh

# for pid in $( ps aux | grep start-asyncssh-server | grep -v grep | awk '{ print $2; }' ); do
#     kill "$pid"; sleep 0.1; kill -9 "$pid"; done


async def start_server():
    await asyncssh.listen(
        "127.0.0.1",
        8022,
        server_host_keys=["ssh_host_key"],
        authorized_client_keys="ssh_user_ca",
        sftp_factory=True,
        allow_scp=True,
    )


loop = asyncio.new_event_loop()
loop.run_until_complete(start_server())
loop.run_forever()
