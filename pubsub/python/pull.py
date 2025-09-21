# python -m pip install google-cloud-pubsub

from google.cloud import pubsub_v1

from gcp_config import PROJECT_ID, SUBSCRIPTION_ID

def main():
    subscriber = pubsub_v1.SubscriberClient()

    # 构建完整的订阅路径
    # projects/{project_id}/subscriptions/{subscription_id}
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    # 同步拉取一批消息（最多 5 条），拿到就立即 ack
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 5}
    )

    if not response.received_messages:
        print("No messages.")
        return

    # 需要ack的消息ID列表
    ack_ids = []
    for m in response.received_messages:
        data = m.message.data.decode("utf-8", errors="replace")
        attrs = dict(m.message.attributes)
        print("=== MESSAGE ===")
        print("id:", m.message.message_id)
        print("publish_time:", m.message.publish_time)
        print("data:", data)
        print("attributes:", attrs)
        ack_ids.append(m.ack_id)

    # 一次性确认（ack）刚才处理的消息
    subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
    print(f"Acked {len(ack_ids)} message(s).")

if __name__ == "__main__":
    main()
