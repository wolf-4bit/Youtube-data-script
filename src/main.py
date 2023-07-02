from youtubesearchpython.__future__ import Suggestions, CustomSearch, VideoSortOrder
import aiofiles
import csv
import aiocsv
import asyncio
import json

async def json_values():
    async with aiofiles.open("Settings.json", "r+") as f:
        content = await f.read()
        print(content)
        details = json.loads(content)
        return details
    
async def fetch_videos(tag, settings):
    videos = []
    video = CustomSearch(tag, VideoSortOrder.uploadDate, limit=20)
    result = await video.next()
    
    for i in range(settings["PAGES_LIMIT"]):
        for item in result["result"]:
            row = {key: item[key] for key in item if key in settings["HEADERS"]}
            videos.append(row)
        result = await video.next()
    
    return videos

async def main():
    tags_task = Suggestions.get('indus game', language='en')
    settings_task = json_values()
    
    tags, settings = await asyncio.gather(tags_task, settings_task)
    videos_tasks = [fetch_videos(tag, settings) for tag in tags['result'][0:settings["TAGS_LIMIT"]]]
    videos_results = await asyncio.gather(*videos_tasks)
    
    for tag, videos in zip(tags['result'][0:settings["TAGS_LIMIT"]], videos_results):
        async with aiofiles.open(f"dataset/{tag}.csv", mode="w+", encoding="utf-8", newline="") as afp:
            writer = aiocsv.AsyncDictWriter(afp, settings["HEADERS"], restval="NULL", quoting=csv.QUOTE_ALL)
            await writer.writerows(videos)
        print(videos)
        
asyncio.run(main())