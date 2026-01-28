import { useHealth } from '@/api/hooks'
import { Badge } from '@/components/ui/badge'

interface HeaderProps {
  title: string
}

export function Header({ title }: HeaderProps) {
  const { data: health, isError, isLoading } = useHealth()

  return (
    <header className="border-b border-border bg-card px-6 py-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">{title}</h1>
        <div className="flex items-center gap-4">
          {isLoading ? (
            <Badge variant="secondary">Connecting...</Badge>
          ) : isError ? (
            <Badge variant="destructive">Disconnected</Badge>
          ) : (
            <Badge variant="success">{health?.status || 'Connected'}</Badge>
          )}
          {health && (
            <span className="text-sm text-muted-foreground">
              v{health.version}
            </span>
          )}
        </div>
      </div>
    </header>
  )
}
