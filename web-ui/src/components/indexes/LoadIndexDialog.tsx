import { useState } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useLoadIndex } from '@/api/hooks'
import { FolderOpen } from 'lucide-react'

export function LoadIndexDialog() {
  const [open, setOpen] = useState(false)
  const [indexId, setIndexId] = useState('')
  const [indexPath, setIndexPath] = useState('')

  const loadMutation = useLoadIndex()

  const handleLoad = () => {
    if (!indexId || !indexPath) {
      alert('Index ID and path are required')
      return
    }

    loadMutation.mutate(
      { indexId, indexPath },
      {
        onSuccess: () => {
          setOpen(false)
          setIndexId('')
          setIndexPath('')
        },
        onError: (error) => {
          alert(`Failed to load index: ${error.message}`)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline">
          <FolderOpen className="mr-2 h-4 w-4" />
          Load Index
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Load Index from Disk</DialogTitle>
          <DialogDescription>
            Load an existing HNSW index from a file path on the server.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid gap-2">
            <label className="text-sm font-medium">Index ID</label>
            <Input
              placeholder="my-index"
              value={indexId}
              onChange={(e) => setIndexId(e.target.value)}
            />
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">Index Path</label>
            <Input
              placeholder="/path/to/index.hnsw"
              value={indexPath}
              onChange={(e) => setIndexPath(e.target.value)}
            />
            <p className="text-xs text-muted-foreground">
              Path to the .hnsw index file on the server
            </p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleLoad} disabled={loadMutation.isPending}>
            {loadMutation.isPending ? 'Loading...' : 'Load Index'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
