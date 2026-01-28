import { Link } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useIndexes } from '@/api/hooks'
import { Database, ArrowRight } from 'lucide-react'

export function IndexList() {
  const { data: indexList, isLoading, isError } = useIndexes()

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          Loaded Indexes
        </CardTitle>
        <Button variant="ghost" size="sm" asChild>
          <Link to="/indexes">
            View All <ArrowRight className="ml-2 h-4 w-4" />
          </Link>
        </Button>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <p className="text-muted-foreground">Loading...</p>
        ) : isError ? (
          <p className="text-destructive">Failed to load indexes</p>
        ) : indexList?.indexes.length === 0 ? (
          <p className="text-muted-foreground">No indexes loaded</p>
        ) : (
          <div className="space-y-2">
            {indexList?.indexes.slice(0, 5).map((index) => (
              <div
                key={index.indexId}
                className="flex items-center justify-between rounded-md border p-3"
              >
                <div>
                  <p className="font-medium">{index.indexId}</p>
                  <p className="text-sm text-muted-foreground">
                    {index.dimension}D, {index.size.toLocaleString()} vectors
                  </p>
                </div>
                <Badge variant="secondary">
                  {index.distanceType || 'euclidean'}
                </Badge>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
