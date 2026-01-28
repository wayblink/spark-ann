import { useState } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useAddVectors, useSaveIndex } from '@/api/hooks'
import type { IndexInfo, VectorData } from '@/types/api'

interface IndexDetailsDialogProps {
  index: IndexInfo | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function IndexDetailsDialog({
  index,
  open,
  onOpenChange,
}: IndexDetailsDialogProps) {
  const [vectorsInput, setVectorsInput] = useState('')
  const [savePath, setSavePath] = useState('')

  const addVectorsMutation = useAddVectors(index?.indexId || '')
  const saveMutation = useSaveIndex(index?.indexId || '')

  const handleAddVectors = () => {
    if (!vectorsInput.trim()) {
      alert('Please enter vectors to add')
      return
    }

    try {
      const vectors: VectorData[] = JSON.parse(vectorsInput)
      if (!Array.isArray(vectors)) {
        alert('Vectors must be an array')
        return
      }
      addVectorsMutation.mutate(
        { vectors },
        {
          onSuccess: () => {
            setVectorsInput('')
            alert('Vectors added successfully')
          },
          onError: (error) => {
            alert(`Failed to add vectors: ${error.message}`)
          },
        }
      )
    } catch {
      alert('Invalid JSON format')
    }
  }

  const handleSave = () => {
    if (!savePath.trim()) {
      alert('Please enter a save path')
      return
    }

    saveMutation.mutate(
      { path: savePath },
      {
        onSuccess: () => {
          setSavePath('')
          alert('Index saved successfully')
        },
        onError: (error) => {
          alert(`Failed to save index: ${error.message}`)
        },
      }
    )
  }

  if (!index) return null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Index: {index.indexId}</DialogTitle>
          <DialogDescription>View details and manage the index</DialogDescription>
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
            <Badge variant="secondary">{index.distanceType || 'euclidean'}</Badge>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Path</p>
            <p className="truncate font-medium">{index.indexPath || '-'}</p>
          </div>
        </div>

        <Tabs defaultValue="add">
          <TabsList className="w-full">
            <TabsTrigger value="add" className="flex-1">
              Add Vectors
            </TabsTrigger>
            <TabsTrigger value="save" className="flex-1">
              Save Index
            </TabsTrigger>
          </TabsList>

          <TabsContent value="add" className="space-y-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium">Vectors (JSON array)</label>
              <Textarea
                placeholder='[{"id": 1, "vector": [0.1, 0.2, ...]}, ...]'
                value={vectorsInput}
                onChange={(e) => setVectorsInput(e.target.value)}
                className="h-32 font-mono text-sm"
              />
            </div>
            <Button
              onClick={handleAddVectors}
              disabled={addVectorsMutation.isPending}
            >
              {addVectorsMutation.isPending ? 'Adding...' : 'Add Vectors'}
            </Button>
          </TabsContent>

          <TabsContent value="save" className="space-y-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium">Save Path</label>
              <Input
                placeholder="/path/to/save/index.hnsw"
                value={savePath}
                onChange={(e) => setSavePath(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Path where the index will be saved on the server
              </p>
            </div>
            <Button onClick={handleSave} disabled={saveMutation.isPending}>
              {saveMutation.isPending ? 'Saving...' : 'Save Index'}
            </Button>
          </TabsContent>
        </Tabs>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
