import { useState, useEffect } from 'react'
import { Header } from '@/components/layout/Header'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { getApiBaseUrl, setApiBaseUrl } from '@/api/client'
import { useHealth } from '@/api/hooks'
import { useQueryClient } from '@tanstack/react-query'
import { Sun, Moon, Monitor } from 'lucide-react'

type Theme = 'light' | 'dark' | 'system'

function getStoredTheme(): Theme {
  if (typeof window === 'undefined') return 'dark'
  return (localStorage.getItem('theme') as Theme) || 'dark'
}

function applyTheme(theme: Theme) {
  const root = document.documentElement
  if (theme === 'system') {
    const systemDark = window.matchMedia('(prefers-color-scheme: dark)').matches
    root.classList.toggle('dark', systemDark)
  } else {
    root.classList.toggle('dark', theme === 'dark')
  }
}

export function Settings() {
  const queryClient = useQueryClient()
  const [apiUrl, setApiUrl] = useState(getApiBaseUrl())
  const [saved, setSaved] = useState(false)
  const [theme, setTheme] = useState<Theme>(getStoredTheme)
  const { data: health, isError, isLoading, refetch } = useHealth()

  useEffect(() => {
    setApiUrl(getApiBaseUrl())
  }, [])

  useEffect(() => {
    applyTheme(theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  useEffect(() => {
    if (theme === 'system') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
      const handler = () => applyTheme('system')
      mediaQuery.addEventListener('change', handler)
      return () => mediaQuery.removeEventListener('change', handler)
    }
  }, [theme])

  const handleSave = () => {
    setApiBaseUrl(apiUrl)
    setSaved(true)
    queryClient.invalidateQueries()
    setTimeout(() => setSaved(false), 2000)
  }

  const handleTest = () => {
    setApiBaseUrl(apiUrl)
    refetch()
  }

  return (
    <div className="flex flex-col">
      <Header title="Settings" />
      <div className="flex-1 space-y-6 p-6">
        <Card>
          <CardHeader>
            <CardTitle>API Configuration</CardTitle>
            <CardDescription>
              Configure the connection to the Spark-ANN API server
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium">API Base URL</label>
              <div className="flex gap-2">
                <Input
                  value={apiUrl}
                  onChange={(e) => setApiUrl(e.target.value)}
                  placeholder="/api/v1"
                />
                <Button variant="outline" onClick={handleTest}>
                  Test
                </Button>
                <Button onClick={handleSave}>
                  {saved ? 'Saved!' : 'Save'}
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Default: /api/v1 (uses Vite proxy to localhost:8080)
              </p>
            </div>

            <div className="flex items-center gap-2">
              <span className="text-sm font-medium">Connection Status:</span>
              {isLoading ? (
                <Badge variant="secondary">Testing...</Badge>
              ) : isError ? (
                <Badge variant="destructive">Connection Failed</Badge>
              ) : (
                <Badge variant="success">Connected ({health?.status})</Badge>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Appearance</CardTitle>
            <CardDescription>
              Customize the look and feel of the application
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium">Theme</label>
              <div className="flex gap-2">
                <Button
                  variant={theme === 'light' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setTheme('light')}
                  className="flex-1"
                >
                  <Sun className="mr-2 h-4 w-4" />
                  Light
                </Button>
                <Button
                  variant={theme === 'dark' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setTheme('dark')}
                  className="flex-1"
                >
                  <Moon className="mr-2 h-4 w-4" />
                  Dark
                </Button>
                <Button
                  variant={theme === 'system' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setTheme('system')}
                  className="flex-1"
                >
                  <Monitor className="mr-2 h-4 w-4" />
                  System
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>About</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <p className="text-sm text-muted-foreground">
              Spark-ANN Web UI - A web interface for managing and querying HNSW
              vector indexes.
            </p>
            {health && (
              <p className="text-sm text-muted-foreground">
                API Version: {health.version}
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
