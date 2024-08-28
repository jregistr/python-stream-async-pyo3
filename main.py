import stream_q
import asyncio

app_id = "<App ID>"


async def main():
    client = await stream_q.new_q_client(app_id)
    chat_stream = await client.chat("Explain what Kendra does?", None, None)
    accumulate = ""
    async for next_chat in chat_stream:
        if next_chat.text is not None:
            print(next_chat.text)
            accumulate += next_chat.text
        else:
            print(next_chat)

    print(f"Whole Text: {accumulate}")

asyncio.run(main())