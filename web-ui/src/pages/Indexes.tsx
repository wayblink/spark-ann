import { Header } from '@/components/layout/Header'
import { IndexTable } from '@/components/indexes/IndexTable'
import { LoadBundleDialog } from '@/components/indexes/LoadBundleDialog'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useIndexes } from '@/api/hooks'

export function Indexes() {
  const { data: indexList } = useIndexes()

  return (
    <div className="flex flex-col">
      <Header title="Bundle Management" />
      <div className="flex-1 space-y-6 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold">Loaded Bundles</h2>
            <p className="text-sm text-muted-foreground">
              {indexList?.totalIndexes || 0} bundles with{' '}
              {indexList?.totalVectors.toLocaleString() || 0} total vectors
            </p>
          </div>
          <LoadBundleDialog />
        </div>

        <Card>
          <CardHeader>
            <CardTitle>All Bundles</CardTitle>
          </CardHeader>
          <CardContent>
            <IndexTable />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
