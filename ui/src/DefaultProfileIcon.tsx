/** Generic user-in-circle placeholder (line art), used when no profile photo is set. */

export function DefaultProfileIcon() {
  return (
    <svg viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <circle cx="24" cy="24" r="21" stroke="currentColor" strokeWidth="2.25" />
      <circle cx="24" cy="18.5" r="6.75" stroke="currentColor" strokeWidth="2.25" />
      <path
        d="M11.5 40.5c1.6-9.2 7.8-14.5 12.5-14.5s10.9 5.3 12.5 14.5"
        stroke="currentColor"
        strokeWidth="2.25"
        strokeLinecap="round"
        fill="none"
      />
    </svg>
  );
}
