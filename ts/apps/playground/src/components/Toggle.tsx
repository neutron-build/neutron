import { useState } from "preact/hooks";

interface ToggleProps {
  initialOn?: boolean;
  onLabel?: string;
  offLabel?: string;
}

function Toggle({ 
  initialOn = false, 
  onLabel = "ON", 
  offLabel = "OFF" 
}: ToggleProps) {
  const [isOn, setIsOn] = useState(initialOn);

  return (
    <button
      onClick={() => setIsOn(!isOn)}
      style={`
        padding: 0.5rem 1rem;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        background: ${isOn ? "#00E5A0" : "#333"};
        color: ${isOn ? "#000" : "#fff"};
        transition: all 0.2s;
      `}
    >
      {isOn ? onLabel : offLabel}
    </button>
  );
}

Toggle.displayName = "Toggle";

export default Toggle;
