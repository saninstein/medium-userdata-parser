import aiohttp
from aiohttp.web_exceptions import HTTPNotFound
import asyncio
import re
import json
from . import helpers
from lib.data_worker import DataWorker


MEDIUM_URL = 'https://medium.com/'
BASE_URL = MEDIUM_URL + '{}/latest?format=json&count=3000000'
HIJACKING_PREFIX = '])}while(1);</x>'

PROJECTS_URL = 'http://db.xyz.hcmc.io/data/coins.json'


ITEM_PATTERN = re.compile(r'medium.com/(\@?[\w\.\-\_]+)/?$')


class MediumDataWorker(DataWorker):

	update_frequency = 60 * 10

	def __init__(self, loop=asyncio.get_event_loop()):
		self.loop = loop
		self.session = None
		self.semaphore = asyncio.Semaphore(5)
		self.headers = helpers.chrome_headers(MEDIUM_URL)

	def fetch_data(self):
		self.loop.run_until_complete(self._fetch_data())

	def save(self, coin_id, data):
		if data is None:
			return
		print(coin_id, data)

	async def _fetch_data(self):
		projects = await self.fetch(PROJECTS_URL)
		for project in projects:
			if not ('community' in project and 'medium' in project['community']):
				continue

			info = dict(followers=0, last_post=-1, posts=0, errors=[])
			urls = project['community']['medium']
			for url in urls:
				item = ITEM_PATTERN.findall(url)
				if not item:
					info['errors'].append('No items')
					continue
				item = item[0]
				# aggregate data
				try:
					item_info = await self.get_info(item)
					info['followers'] += item_info['followers']
					info['posts'] += item_info['posts']
					info['last_post'] = max([info['last_post'], item_info['last_post']])
				except HTTPNotFound:
					info['errors'].append((url, 'Not Found'))

			info['last_post'] = info['last_post'] if info['last_post'] != -1 else None

			if not info['errors']:
				info.pop('errors', None)

			self.save(project['id'], info)

	async def get_info(self, item):
		res = await self.fetch(BASE_URL.format(item))
		data = res['payload']
		
		if item.startswith('@'):
			#get data from user
			user_id = data['user']['userId']
			return {
				'last_post': data['user']['lastPostCreatedAt'],
				'posts': data['userMeta']['numberOfPostsPublished'],
				'followers': data['references']['SocialStats'][user_id]['usersFollowedByCount']
			}
		else:
			#get data from collection
			return {
				'last_post': max([x['createdAt'] for x in data['posts']]) if data['posts'] else -1,
				'posts': len(data['posts']),
				'followers': data['collection']['metadata']['followerCount']
			}

	async def close_session(self):
		if self.session:
			await self.session.close()
			self.session = None

	async def fetch(self, url):
		if self.session is None:
			self.session = aiohttp.ClientSession(headers=self.headers)

		async with self.semaphore:
			async with self.session.get(url) as res:
				if res.status in (404, 302):  # 302 - User is blacklisted
					raise HTTPNotFound
				text = await res.text()

				# response data has hijacking protection:)
				text = text.replace(HIJACKING_PREFIX, '')
				return json.loads(text)

