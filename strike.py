from datetime import datetime, timedelta
import dateutil
import requests
import math
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

class StrikeList():
    def __init__(self, source, limit=None, host='', sponsor=''):
        self.source = source
        self.limit = limit
        self.strike_data = self.get_source()
        self.list = []
        self.host = host
        self.sponsor = sponsor
        self.postcodesio()
        return
    
    def get_source(self):
        data = requests.get(self.source).json()['pageProps']['staticStrikes']
        if isinstance(self.limit, int):
            return data[0:self.limit]
        print(f"Retreived {len(data)} strikes from the strike map")
        return data
    
    def postcodesio(self):
        limit = 100
        chunked_strike_data = []
        no_chunks = math.ceil(len(self.strike_data) / limit)
        for i in range(no_chunks):
            this_chunk_data = self.strike_data[i*limit:(i+1)*limit]
            chunked_strike_data.append(this_chunk_data)
        pre_geocoded = []
        for chunk in chunked_strike_data:
            geocode_data = [{"longitude": e['location']['lng'], "latitude": e['location']['lat']} for e in chunk]
            postcode_requests = requests.post('https://api.postcodes.io/postcodes', json={'geolocations': geocode_data})
            postcode_data = postcode_requests.json()
            postcodes = postcode_data['result']
            for i in range(len(postcodes)):
                strike = Strike(chunk[i], postcodes[i], host=self.host, sponsor=self.sponsor)
                self.list.append(strike)
                pre_geocoded.append({'id': strike.id, 'geom': strike.geom})
        print("Encoded strikes from postcdoes.io")
        pre_geocoded_df = pd.DataFrame(pre_geocoded)
        self.nominatim_encode(pre_geocoded_df)
    
    def nominatim_encode(self, pre_geocoded_df):
        if len(pre_geocoded_df) < 1:
            return
        df = pre_geocoded_df
        locator = Nominatim(user_agent="myGeocoder", timeout=10)
        rgeocode = RateLimiter(locator.reverse, min_delay_seconds=0.001)
        df['address'] = df["geom"].apply(rgeocode)
        self.nominatim_encoded_df = df
        df.to_csv('testing.csv')
        self.match_geocoding()
        return df
    
    def match_geocoding(self):
        addresses_df = self.nominatim_encoded_df
        for i in range(len(self.list)):
            strike = self.list[i]
            addresses = addresses_df.loc[addresses_df['id'] == strike.id]['address'].to_list()
            if len(addresses) > 0:
                address = addresses[0]
                self.list[i].address = address.raw
    
    def an_events(self):
        events = []
        for strike in self.list:
            events.append(strike.an_event())
        return events
    
    def to_list(self):
        strikes = []
        for strike in self.list:
            strikes.append(strike.to_dict())
        return strikes

    def to_csv(self, path_or_buf, **kwargs):
        strike_list = self.to_list()
        strike_list_df = pd.json_normalize(strike_list)
        strike_list_df.to_csv(path_or_buf, **kwargs)
    
    def an_event_csv(self, path_or_buf, **kwargs):
        strike_list = self.an_events()
        strike_list_df = pd.json_normalize(strike_list)
        strike_list_df.to_csv(path_or_buf, **kwargs)


class Strike():
    def __init__(self, raw, postcodes=[], host='', sponsor=''):
        self.raw = raw
        self.id = self.raw['id']
        self.postcodes = postcodes['result']
        self.host = host
        self.sponsor = sponsor
        self.postcode_q = postcodes['query']
        self.postcode = self.get_postcode()
        self.lat = self.raw['location']['lat']
        self.lng = self.raw['location']['lng']
        self.geom = f"{self.lat},{self.lng}"
    
    def get_postcode(self):
        if not self.postcodes:
            return {}
        if len(self.postcodes) > 0:
            return self.postcodes[0]
    
    def to_dict(self):
        return {
            **self.raw,
            'postcode': self.postcode,
            'address': self.address['address']
        }
    
    def an_event(self):
        action_date = dateutil.parser.parse(self.raw['action_start'])
        if datetime.timestamp(action_date) < datetime.timestamp(datetime.now()):
            action_date = datetime.now() + timedelta(days=10)
        adr = self.address['address']
        city = adr.get('city') or adr.get('suburb') or adr.get('town') or adr.get('village') or adr.get('county')
        an_adr = f"{adr.get('house_number') or ''} {adr.get('road')}".strip()
        return {
            'event_title': f"{self.raw['trade_unions_taking_action']} against {self.raw['employer_name']} about {self.raw['action_reason']}",
            'administrative_title': f"StrikeID: {self.raw['id']}",
            'location_name': adr.get('place') or adr.get('shop') or adr.get('building') or city,
            'address': an_adr,
            'city': city,
            'state': adr.get('county'),
            'zip': self.postcode.get('postcode'),
            'country': adr.get('country_code').upper() or 'GB',
            'time': action_date.strftime("%H:%M"),
            'date': action_date.strftime("%m/%d/%Y"),
            'host': self.host,
            'sponsor': self.sponsor,
            'attendee_pitch': self.raw['more_information'],
            'attendee_instructions': self.raw['more_information'],
            'host_contact_info':  self.raw['email_solidarity'],
        }

if __name__=="__main__":
    print('Wow the name is main lolz')
