import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import type { UnifiedIndexEntry } from '@/types/api'

interface IndexDetailsDialogProps {
  index: UnifiedIndexEntry | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function IndexDetailsDialog({ index, open, onOpenChange }: IndexDetailsDialogProps) {
  if (!index) return null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Index: {index.indexId}</DialogTitle>
          <DialogDescription>Bundle metadata and routing summary</DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-2 gap-4 py-4">
          <div>
            <p className="text-sm text-muted-foreground">Dimension</p>
            <p className="font-medium">{index.dimension}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Vectors</p>
            <p className="font-medium">{index.size.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Distance Type</p>
            <Badge variant="secondary">{index.distanceType}</Badge>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Path</p>
            <p className="truncate font-medium">{index.bundlePath || '-'}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Local Indexes</p>
            <p className="font-medium">{index.numLocalIndexes ?? 0}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Global Routing</p>
            <p className="font-medium">{index.hasGlobalIndex ? 'Yes' : 'No'}</p>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
