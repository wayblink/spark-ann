import { useState } from 'react'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useLoadBundle } from '@/api/hooks'
import { FolderOpen } from 'lucide-react'

export function LoadBundleDialog() {
  const [open, setOpen] = useState(false)
  const [indexId, setIndexId] = useState('')
  const [bundlePath, setBundlePath] = useState('')
  const loadMutation = useLoadBundle()

  const handleLoad = () => {
    if (!indexId || !bundlePath) {
      alert('Bundle ID and path are required')
      return
    }

    loadMutation.mutate(
      { indexId, bundlePath },
      {
        onSuccess: () => {
          setOpen(false)
          setIndexId('')
          setBundlePath('')
        },
        onError: (error) => {
          alert(`Failed to load bundle: ${error.message}`)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline">
          <FolderOpen className="mr-2 h-4 w-4" />
          Load Bundle
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Load Bundle from Disk</DialogTitle>
          <DialogDescription>
            Load a Spark-built ANN bundle from a directory path on the server.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid gap-2">
            <label className="text-sm font-medium">Bundle ID</label>
            <Input placeholder="my-bundle" value={indexId} onChange={(e) => setIndexId(e.target.value)} />
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">Bundle Path</label>
            <Input placeholder="/path/to/bundle" value={bundlePath} onChange={(e) => setBundlePath(e.target.value)} />
            <p className="text-xs text-muted-foreground">Path to the bundle root directory on the server</p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
          <Button onClick={handleLoad} disabled={loadMutation.isPending}>
            {loadMutation.isPending ? 'Loading...' : 'Load Bundle'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
