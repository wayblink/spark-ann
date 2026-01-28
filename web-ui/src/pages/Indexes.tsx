import { Header } from '@/components/layout/Header'
import { IndexTable } from '@/components/indexes/IndexTable'
import { CreateIndexDialog } from '@/components/indexes/CreateIndexDialog'
import { LoadIndexDialog } from '@/components/indexes/LoadIndexDialog'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useIndexes } from '@/api/hooks'

export function Indexes() {
  const { data: indexList } = useIndexes()

  return (
    <div className="flex flex-col">
      <Header title="Index Management" />
      <div className="flex-1 space-y-6 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold">Loaded Indexes</h2>
            <p className="text-sm text-muted-foreground">
              {indexList?.totalIndexes || 0} indexes with{' '}
              {indexList?.totalVectors.toLocaleString() || 0} total vectors
            </p>
          </div>
          <div className="flex gap-2">
            <LoadIndexDialog />
            <CreateIndexDialog />
          </div>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>All Indexes</CardTitle>
          </CardHeader>
          <CardContent>
            <IndexTable />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
