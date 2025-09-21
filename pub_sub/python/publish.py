import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1

from gcp_config import PROJECT_ID, TOPIC_ID

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    total_seconds = 60
    interval = 5
    rounds = total_seconds // interval  # 12 次

    print(f"Publishing to {topic_path} every {interval}s for {total_seconds}s ...")
    for i in range(rounds):
        now = datetime.now(timezone.utc).isoformat()
        payload = f"hello {now}"                     # 字符串
        data = payload.encode("utf-8")               # 转 bytes

        # 可选：添加一些属性
        future = publisher.publish(
            topic_path,
            data,
            lang="zh",
            source="local-script",
            index=str(i + 1),
        )
        msg_id = future.result(timeout=30)
        print(f"[{i+1}/{rounds}] published id={msg_id} data={payload}")

        if i < rounds - 1:
            time.sleep(interval)

    print("Done.")

if __name__ == "__main__":
    main()
