from kh_common.server import Request, Response, ServerApp, StreamingResponse
from kh_common.exceptions.http_error import Forbidden
from kh_common.models.auth import Scope
from kh_common.caching import ArgsCache
import aiohttp


app = ServerApp()
timeout: float = 30
host = 'https://f002.backblazeb2.com/file/kheina-content/'


async def genny(method, url, headers={ }, body=None) :
	async with aiohttp.request(
		method,
		url,
		headers=headers,
		data=body,
		timeout=aiohttp.ClientTimeout(30),
	) as response :
		yield response.headers, response.status
		async for data, _ in response.content.iter_chunks():
			yield data


@app.get('/')
async def home() :
	return {
		'description': 'this service serves data from the cdn by first authenticating the user and then streaming the response from the host.',
	}


@app.get('{path:path}')
async def all_routes(req: Request, path: str) :
	await req.user.verify_scope(Scope.admin)

	response = genny(req.method, host + path.lstrip('/'), req.headers, await req.body())
	headers, status = await response.__anext__()

	if headers.get('content-length') :
		return StreamingResponse(
			response,
			headers=headers,
			status_code=status,
		)

	else :
		return Response(
			None,
			headers=headers,
			status_code=status,
		)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5007)
