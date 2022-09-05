from strike import StrikeList
from datetime import datetime
import yaml

settings = yaml.load(open('settings.yaml', 'r'))


strike_list = StrikeList(
    settings['source'],
    host=settings['host'],
    sponsor=settings['source'],
    # limit=100 # for testing sample uploads
    )
strike_list.to_csv(f'output/geocoded/{datetime.now()}strikemap_loads.csv', index=False)
strike_list.an_event_csv(f'output/an_events/{datetime.now()}strikemap.csv', index=False)
