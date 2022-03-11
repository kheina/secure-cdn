from kh_common.server import Request, Response, ServerApp, StreamingResponse
from kh_common.exceptions.http_error import BadRequest, NotFound
from kh_common.config.constants import posts_host
from aiohttp import ClientTimeout, request
from kh_common.models.auth import Scope
from asyncio import ensure_future
from pydantic import constr


app = ServerApp()
timeout: float = 30
host = 'https://f002.backblazeb2.com/file/kheina-content/'
public_privacy = { 'public', 'unlisted' }


async def jenny(method, url, headers={ }, body=None) :
	# TODO: pyc this?
	async with request(
		method,
		url,
		headers=headers,
		data=body,
		timeout=ClientTimeout(timeout),
	) as response :
		yield response.headers, response.status
		async for data, _ in response.content.iter_chunks():
			yield data


def throw_not_found() :
	raise NotFound('The requested resource is not available or does not exist.')


async def fetch_post(post_id: str) :
	# TODO: replace this function with (Avro) Gateway
	async with request(
		'GET',
		f'{posts_host}/v1/post/{post_id}',
		timeout=ClientTimeout(timeout),
		headers={
			'authorization': 'bearer {internal_token}',
		},
	) as response :
		if response.status >= 400 :
			throw_not_found()

		return await response.json()


async def validate_user_permissions(post, user) :
	authenticated = ensure_future(user.verify_scope(Scope.mod, raise_error=False))

	if post['privacy'] in public_privacy :
		return

	if await authenticated :
		return

	# TODO: add a third check here to see if the user is the uploader

	# TODO: add a fourth check here to see if the user was given explicit permission to view the post

	throw_not_found()


@app.get('/')
async def home() :
	return {
		'description': 'this service serves data from the cdn by first authenticating the user and then streaming the response from the host.',
	}


@app.get('/file/{file:path}')
async def b2_path() :
	raise BadRequest('the url you have entered is not currently supported')


@app.get('{post_id}/{file:path}')
async def media(req: Request, post_id: constr(min_length=8, max_length=8), file: str) :
	post = ensure_future(fetch_post(post_id))
	response = jenny(req.method, host + file.lstrip('/'), req.headers, await req.body())
	response_headers = ensure_future(response.__anext__())

	await validate_user_permissions(await post, req.user)

	headers, status = await response_headers

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
