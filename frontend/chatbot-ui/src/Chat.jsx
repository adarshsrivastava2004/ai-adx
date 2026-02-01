import { useEffect, useRef, useState } from "react";
import Message from "./Message";

const API_URL = "http://127.0.0.1:8000/chat";

export default function Chat() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  async function sendMessage() {
    if (!input.trim() || loading) return;

    const userText = input;
    setMessages(prev => [...prev, { role: "user", text: userText }]);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: userText })
      });

      const data = await res.json();
      setMessages(prev => [...prev, { role: "bot", text: data.reply }]);
    } catch {
      setMessages(prev => [...prev, { role: "bot", text: "Server not reachable 😕" }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="chat-root">
      <div className="chat-header">Storm Events Assistant</div>

      <div className="chat-body">
        {messages.map((m, i) => (
          <Message key={i} role={m.role} text={m.text} />
        ))}

        {loading && <Message role="bot" text="Typing…" typing />}
        <div ref={bottomRef} />
      </div>

      <div className="chat-input">
        <textarea
          rows="1"
          value={input}
          placeholder="Send a message…"
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === "Enter" && !e.shiftKey && (e.preventDefault(), sendMessage())}
        />
        <button onClick={sendMessage} disabled={loading}>
          ➤
        </button>
      </div>
    </div>
  );
}
