# Spark-ANN Web UI

A React-based web interface for managing and querying Spark-ANN vector indexes.

## Features

- **Dashboard**: Health stats, loaded indexes overview, and quick search
- **Index Management**: Create, load, delete indexes; add vectors; save to disk
- **Search**: Single index, multi-index, and batch search modes
- **Settings**: API configuration and theme switching (light/dark/system)
- **Demo Data**: Generate random vectors for quick testing

## Tech Stack

- React 18 + TypeScript
- Vite (build tool)
- Tailwind CSS + shadcn/ui components
- TanStack Query (API state management)
- React Router v6 (routing)
- Axios (HTTP client)

## Prerequisites

- Node.js 18+
- npm or yarn
- Spark-ANN API server running on port 8080

## Getting Started

### Install Dependencies

```bash
npm install
```

### Development

Start the development server with hot reload:

```bash
npm run dev
```

The UI will be available at http://localhost:5173

The Vite dev server proxies `/api` requests to `http://localhost:8080` automatically.

### Production Build

```bash
npm run build
```

Build output will be in the `dist/` directory.

### Preview Production Build

```bash
npm run preview
```

## Project Structure

```
web-ui/
├── src/
│   ├── main.tsx              # Entry point
│   ├── App.tsx               # Root component with router
│   ├── index.css             # Global styles + Tailwind
│   ├── api/
│   │   ├── client.ts         # Axios instance
│   │   └── hooks.ts          # TanStack Query hooks
│   ├── types/
│   │   └── api.ts            # TypeScript types matching API
│   ├── lib/
│   │   └── utils.ts          # Utility functions
│   ├── components/
│   │   ├── ui/               # Base UI components (shadcn/ui)
│   │   ├── layout/           # Layout components
│   │   ├── dashboard/        # Dashboard components
│   │   ├── indexes/          # Index management components
│   │   └── search/           # Search components
│   └── pages/
│       ├── Dashboard.tsx
│       ├── Indexes.tsx
│       ├── Search.tsx
│       └── Settings.tsx
├── index.html
├── package.json
├── vite.config.ts
├── tailwind.config.js
└── tsconfig.json
```

## API Integration

The UI connects to the Spark-ANN REST API. Key endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/health` | Health check |
| `GET /api/v1/indexes` | List all indexes |
| `POST /api/v1/indexes` | Create new index |
| `POST /api/v1/indexes/load` | Load index from disk |
| `DELETE /api/v1/indexes/{id}` | Delete index |
| `POST /api/v1/indexes/{id}/search` | Search single index |
| `POST /api/v1/search` | Multi-index search |
| `POST /api/v1/search/batch` | Batch search |

See the [API documentation](../docs/design/api-design.md) for full details.

## Configuration

### API URL

By default, the UI connects to `/api/v1` (proxied to localhost:8080 in development).

To change the API URL:
1. Go to Settings page
2. Update the "API Base URL" field
3. Click "Test" to verify connection
4. Click "Save" to persist

### Theme

The UI supports three theme modes:
- **Light**: Light background with dark text
- **Dark**: Dark background with light text (default)
- **System**: Follows your OS preference

Theme preference is saved to localStorage.

## Available Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start development server |
| `npm run build` | Build for production |
| `npm run preview` | Preview production build |
| `npm run lint` | Run ESLint |

## Environment Variables

Create a `.env.local` file for local overrides:

```env
# Override API proxy target (optional)
VITE_API_URL=http://localhost:8080
```

## Docker

Build and run with Docker:

```bash
# Build image
docker build -t spark-ann-web-ui .

# Run container
docker run -p 80:80 spark-ann-web-ui
```

Or use docker-compose from the project root:

```bash
docker-compose up web-ui
```

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## License

See the main project [LICENSE](../LICENSE) file.
