import json
import logging
import re
import time
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import time

from flask import Flask, Response, jsonify, request


class BackendServer:
    def __init__(self):
        self._app = Flask(__name__)
        self._app.add_url_rule('/up', 'up', self.up)
        self._app.add_url_rule('/get_tick', 'get_tick', self.return_tick)
        self._app.add_url_rule('/get_timeline', 'get_timeline', self._get_ticked_timeline)
        self._app.add_url_rule('/reset_tick', 'reset_tick', self.reset_counter)


        self.time_series = pd.read_csv("time_series.csv")
        self.time_series["unix"] = pd.to_datetime(self.time_series["time"]).astype(np.int64) // 10**6
        self.counter=0
        self.max_counter=96*5

        # self._app.register_error_handler(Exception, self.handle_error_generic)
        # self._app.register_error_handler(HTTPException, self.handle_error_http)

        self.timeline = []

        self.plants = self.time_series.name.unique()
        self.tick_timeline = {plant: None for plant in self.plants}

        self.time_series.drop(columns=["unix"], inplace=True)
        self.time_series["time"] = pd.to_datetime(self.time_series["time"])

        self.bedarfe = pd.read_csv("bedarfe.csv")


    def _init_timeline(self):
        timeline = []
        last_val = 0
        last_name = self.time_series.iloc[0]["name"]
        cur_active = False
        last_time = time.mktime(pd.to_datetime("2021-06-01 21:45:00").timetuple())
        for index, row in self.time_series.iterrows():
            if (row.command != last_val) | ((row.command != 0) & (row["name"] != last_name)):
                if not cur_active:
                    cur_active = True
                    start_time = row.unix
                    command = row.command
                else:
                    cur_active = False
                    stop_time = last_time
                    timeline.append({"start": start_time, "finish":stop_time, "value":command, "name":last_name})
            last_val = row.command
            last_name = row["name"]
            last_time = row.unix
        return timeline


    def run(self):
        self._app.run(host="0.0.0.0", port=8890, threaded=True)

    def up(self):
        return "OK"

    def _get_ticked_timeline(self):
        response =  jsonify(self.timeline + [x for x in self.tick_timeline.values() if x is not None])
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


    def _process_tick_for_timeline(self, data):
        # print(data)
        for index, row in data.iterrows():
            if row["command"] != 0:
                if self.tick_timeline[row["name"]] is None:
                    self.tick_timeline[row["name"]] = {
                        "start": time.mktime(self.time_series.iloc[self.counter]["time"].timetuple()) * 10**3,
                        "finish": time.mktime(self.time_series.iloc[self.counter]["time"].timetuple()) * 10**3,
                        "name": row["name"],
                        "value": row["command"]
                    }
                else:
                    self.tick_timeline[row["name"]]["finish"] = time.mktime(self.time_series.iloc[self.counter]["time"].timetuple()) * 10**3
            else:
                if self.tick_timeline[row["name"]] is None:
                    continue
                else:
                    self.timeline.append(self.tick_timeline[row["name"]])
                    self.tick_timeline[row["name"]] = None
        return

    def return_tick(self):
        if self.counter == self.max_counter:
            response = jsonify({"message": "No more data"})
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response
        data = self.time_series[self.time_series["time"] == self.time_series.iloc[self.counter]["time"]]
        # print(data.columns)
        # data = self.time_series.iloc[self.counter].to_json()
        # print(data.ist.sum()+ self.bedarfe.iloc[self.counter].values)
        response = jsonify({
            "time": time.mktime(self.time_series.iloc[self.counter]["time"].timetuple()) * 10**3,
            "PowerPlants": data[["name", "ist", "pot_plus", "pot_minus", "command"]].to_dict(orient="records"),
            "NetStates": [{
                "name": "Netzbetreiber Mitte",
                "ist": data.ist.sum(),
                "pot_plus": data.pot_plus.sum(),
                "pot_minus": data.pot_minus.sum(),
                "soll": data.ist.sum() + self.bedarfe.iloc[self.counter].bedarfe
            }]
        })

        self._process_tick_for_timeline(data)
        self.counter += 1
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response

    def reset_counter(self):
        self.counter = 0
        response = jsonify(message="done")
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response

    def get_timeline(self):
        response = jsonify(self.timeline)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


if __name__=="__main__":

    api = BackendServer()
    api.run()

