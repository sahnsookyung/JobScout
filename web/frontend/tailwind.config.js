/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            width: {
                sidebar: '360px',
                'sidebar-content': 'calc(360px - 3rem)', // 360px - 48px (p-6 = 1.5rem * 2)
            },
            maxWidth: {
                sidebar: '360px',
            },
            gridTemplateColumns: {
                'main-sidebar': '1fr 360px',
            },
        },
    },
    plugins: [],
}
