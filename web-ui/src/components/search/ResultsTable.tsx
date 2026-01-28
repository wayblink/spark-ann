import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { SearchResultItem, MergedSearchResultItem } from '@/types/api'

interface ResultsTableProps {
  results: SearchResultItem[] | MergedSearchResultItem[]
  queryTimeMs?: number
  showIndexId?: boolean
  title?: string
}

function isMergedResult(
  result: SearchResultItem | MergedSearchResultItem
): result is MergedSearchResultItem {
  return 'indexId' in result
}

export function ResultsTable({
  results,
  queryTimeMs,
  showIndexId = false,
  title = 'Search Results',
}: ResultsTableProps) {
  if (!results.length) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">No results to display</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle className="text-lg">{title}</CardTitle>
        {queryTimeMs !== undefined && (
          <span className="text-sm text-muted-foreground">
            {queryTimeMs}ms | {results.length} results
          </span>
        )}
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-16">Rank</TableHead>
              <TableHead>Vector ID</TableHead>
              {showIndexId && <TableHead>Index</TableHead>}
              <TableHead className="text-right">Distance</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {results.map((result, index) => (
              <TableRow key={`${isMergedResult(result) ? result.indexId : ''}-${result.id}-${index}`}>
                <TableCell className="font-medium">{index + 1}</TableCell>
                <TableCell>{result.id}</TableCell>
                {showIndexId && isMergedResult(result) && (
                  <TableCell>{result.indexId}</TableCell>
                )}
                <TableCell className="text-right font-mono">
                  {result.distance.toFixed(6)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
