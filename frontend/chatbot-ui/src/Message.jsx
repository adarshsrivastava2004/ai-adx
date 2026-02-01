export default function Message({ role, text, typing }) {
  return (
    <div className={`message-row ${role}`}>
      <div className={`message-bubble ${typing ? "typing" : ""}`}>
        {text}
      </div>
    </div>
  );
}
