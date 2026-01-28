import { Header } from '@/components/layout/Header'
import { StatsCard } from '@/components/dashboard/StatsCard'
import { QuickSearch } from '@/components/dashboard/QuickSearch'
import { IndexList } from '@/components/dashboard/IndexList'
import { useHealth, useIndexes } from '@/api/hooks'
import { Database, Hash, Activity, Server } from 'lucide-react'

export function Dashboard() {
  const { data: health, isLoading: healthLoading } = useHealth()
  const { data: indexList, isLoading: indexLoading } = useIndexes()

  return (
    <div className="flex flex-col">
      <Header title="Dashboard" />
      <div className="flex-1 space-y-6 p-6">
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <StatsCard
            title="Status"
            value={healthLoading ? '...' : health?.status || 'Unknown'}
            description={health ? `Version ${health.version}` : undefined}
            icon={Server}
          />
          <StatsCard
            title="Loaded Indexes"
            value={
              indexLoading ? '...' : indexList?.totalIndexes.toLocaleString() || '0'
            }
            description="Active in memory"
            icon={Database}
          />
          <StatsCard
            title="Total Vectors"
            value={
              healthLoading
                ? '...'
                : health?.totalVectors.toLocaleString() || '0'
            }
            description="Across all indexes"
            icon={Hash}
          />
          <StatsCard
            title="API Status"
            value={healthLoading ? 'Checking...' : health ? 'Online' : 'Offline'}
            description="Real-time connection"
            icon={Activity}
          />
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          <IndexList />
          <QuickSearch />
        </div>
      </div>
    </div>
  )
}
