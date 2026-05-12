import os
import requests
import pandas as pd
from typing import Any
from pydantic import BaseModel, Field
from json import dumps
import time
import textwrap
import logging



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class Mb_Client(BaseModel):
    url: str
    username: str
    password: str = Field(repr=False)
    api_key: str = Field(default='', repr=False)
    session_header: dict = Field(default_factory=dict, repr=False)


    def model_post_init(self, __context: Any) -> None:
        self.get_session()


    def get_session(self) -> None:
        # use api_key if available
        # if self.api_key:
        #     setattr(self, "session_header", {"x-api-key": self.api_key})
        #     return

        credentials: dict = {
            "username": self.username,
            "password": self.password
        }
        
        response = requests.post(
            f"{self.url}/api/session",
            json=credentials
        )

        session_id = response.json()["id"]
        setattr(self, "session_header", {"X-Metabase-Session": session_id})


    def _extract_mb_error(self, resp: requests.Response) -> str:
        try:
            js = resp.json()
        except Exception:
            return f"HTTP {resp.status_code}: {resp.text[:1000]}"

        parts = []
        for k in ("message", "error", "cause"):
            if k in js and js[k]:
                parts.append(str(js[k]))
        if isinstance(js.get("errors"), dict) and js["errors"]:
            parts.append(dumps(js["errors"], ensure_ascii=False))
        if js.get("state") == "failed":
            parts.append(str(js.get("error_type", "state=failed")))
        if not parts:
            parts.append(str(js))
        return f"HTTP {resp.status_code}: " + " | ".join(parts)


    def post(self, api_endpoint: str, query: str) -> pd.DataFrame:
        query = textwrap.dedent(query).strip()
        payload: dict = {
            "database": 2,
            "type": "native",
            "format_rows": False,
            "pretty": False,
            "native": {
                "query": query
            }
        }
        for attempt in range(10):
            post = requests.post(
                f'{self.url}/api/{api_endpoint}',
                headers=self.session_header | {
                    "Content-Type": "application/json;charset=utf-8"
                },
                json=payload
            )
            if post.status_code >= 400:
                print(self._extract_mb_error(post))
                time.sleep(5)
            # elif post.status_code == 202:
            #     print("Metabase returned 202 Accepted (query still running). Retrying...")
            #     print(post.json())
            else:
                try:
                    json_res = post.json()
                    # if there are no json_res['data']['cols'] print json_res
                    if 'data' not in json_res or 'cols' not in json_res['data'] or json_res['data']['cols'] == [] or json_res['data']['cols'] is None:
                        print(f"Unexpected response structure: {json_res}")
                        print(f"erorr, reloading")
                        time.sleep(3)
                        continue
                except ValueError as e:
                    print(f"Invalid JSON in response: {post.text[:1000]}")
                if any(k in json_res for k in ("error", "message")) and "data" not in json_res:
                    print(str(json_res.get("error") or json_res.get("message")))
                else:
                    break
        # json_res = post.json()
        column_names = [col['display_name'] for col in json_res['data']['cols']]
        data_rows = json_res['data']['rows']
        df = pd.DataFrame(data_rows, columns=column_names)
        return df
    
    def get_user_by_id(self, id: int) -> dict:
        url = f'{self.url}/api/user/{id}'
        print(url)
        resp = requests.get(
            url,
            headers=self.session_header | {
                "Content-Type": "application/json;charset=utf-8"
            }
        )
        logger.info(f"get_user_by_id response: {resp.status_code} {resp.text[:1000]}")
        print(resp)

        return resp.json()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    mb_client = Mb_Client(
        url=os.environ["METABASE_URL"],
        username=os.environ["METABASE_USERNAME"],
        password=os.environ["METABASE_PASSWORD"],
        api_key=os.environ.get("METABASE_API_KEY", "")
    )

    df = mb_client.post("dataset", "SELECT 1 AS test")
    print(df)
    user = mb_client.get_user_by_id(16)
    print(user)