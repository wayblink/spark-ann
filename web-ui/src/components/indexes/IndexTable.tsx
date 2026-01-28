import { useState } from 'react'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useIndexes, useDeleteIndex } from '@/api/hooks'
import { IndexDetailsDialog } from './IndexDetailsDialog'
import { Trash2, Eye } from 'lucide-react'
import type { IndexInfo } from '@/types/api'

export function IndexTable() {
  const { data: indexList, isLoading, isError } = useIndexes()
  const deleteMutation = useDeleteIndex()
  const [selectedIndex, setSelectedIndex] = useState<IndexInfo | null>(null)

  const handleDelete = (indexId: string) => {
    if (confirm(`Delete index "${indexId}"?`)) {
      deleteMutation.mutate(indexId)
    }
  }

  if (isLoading) {
    return <p className="text-muted-foreground">Loading indexes...</p>
  }

  if (isError) {
    return <p className="text-destructive">Failed to load indexes</p>
  }

  if (!indexList?.indexes.length) {
    return (
      <p className="text-muted-foreground">
        No indexes loaded. Create or load an index to get started.
      </p>
    )
  }

  return (
    <>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Index ID</TableHead>
            <TableHead>Dimension</TableHead>
            <TableHead>Vectors</TableHead>
            <TableHead>Distance Type</TableHead>
            <TableHead>Path</TableHead>
            <TableHead className="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {indexList.indexes.map((index) => (
            <TableRow key={index.indexId}>
              <TableCell className="font-medium">{index.indexId}</TableCell>
              <TableCell>{index.dimension}</TableCell>
              <TableCell>{index.size.toLocaleString()}</TableCell>
              <TableCell>
                <Badge variant="secondary">
                  {index.distanceType || 'euclidean'}
                </Badge>
              </TableCell>
              <TableCell className="max-w-[200px] truncate text-muted-foreground">
                {index.indexPath || '-'}
              </TableCell>
              <TableCell className="text-right">
                <div className="flex justify-end gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setSelectedIndex(index)}
                  >
                    <Eye className="h-4 w-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => handleDelete(index.indexId)}
                    disabled={deleteMutation.isPending}
                  >
                    <Trash2 className="h-4 w-4 text-destructive" />
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      <IndexDetailsDialog
        index={selectedIndex}
        open={!!selectedIndex}
        onOpenChange={(open) => !open && setSelectedIndex(null)}
      />
    </>
  )
}
