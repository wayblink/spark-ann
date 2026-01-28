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
import { Textarea } from '@/components/ui/textarea'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useCreateIndex } from '@/api/hooks'
import { Plus, Sparkles } from 'lucide-react'
import type { VectorData } from '@/types/api'

function generateDemoVectors(count: number, dimension: number): VectorData[] {
  const vectors: VectorData[] = []
  for (let i = 0; i < count; i++) {
    const vector: number[] = []
    for (let j = 0; j < dimension; j++) {
      vector.push(Math.random() * 2 - 1) // Random values between -1 and 1
    }
    vectors.push({ id: i + 1, vector })
  }
  return vectors
}

export function CreateIndexDialog() {
  const [open, setOpen] = useState(false)
  const [indexId, setIndexId] = useState('')
  const [vectorsInput, setVectorsInput] = useState('')
  const [distanceType, setDistanceType] = useState('euclidean')
  const [m, setM] = useState('16')
  const [efConstruction, setEfConstruction] = useState('200')
  const [demoCount, setDemoCount] = useState('100')
  const [demoDimension, setDemoDimension] = useState('128')

  const createMutation = useCreateIndex()

  const handleGenerateDemo = () => {
    const count = parseInt(demoCount) || 100
    const dimension = parseInt(demoDimension) || 128
    const vectors = generateDemoVectors(count, dimension)
    setVectorsInput(JSON.stringify(vectors, null, 2))
  }

  const handleCreate = () => {
    if (!indexId) {
      alert('Index ID is required')
      return
    }

    let vectors: VectorData[] = []
    if (vectorsInput.trim()) {
      try {
        const parsed = JSON.parse(vectorsInput)
        if (!Array.isArray(parsed)) {
          alert('Vectors must be an array')
          return
        }
        vectors = parsed
      } catch {
        alert('Invalid JSON format for vectors')
        return
      }
    }

    createMutation.mutate(
      {
        indexId,
        vectors,
        config: {
          m: parseInt(m) || 16,
          efConstruction: parseInt(efConstruction) || 200,
          distanceType,
        },
      },
      {
        onSuccess: () => {
          setOpen(false)
          setIndexId('')
          setVectorsInput('')
          setDistanceType('euclidean')
          setM('16')
          setEfConstruction('200')
          setDemoCount('100')
          setDemoDimension('128')
        },
        onError: (error) => {
          alert(`Failed to create index: ${error.message}`)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          Create Index
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Create New Index</DialogTitle>
          <DialogDescription>
            Create a new HNSW index with optional initial vectors.
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
          <div className="grid grid-cols-3 gap-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium">Distance Type</label>
              <Select value={distanceType} onValueChange={setDistanceType}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="euclidean">Euclidean</SelectItem>
                  <SelectItem value="cosine">Cosine</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <label className="text-sm font-medium">M</label>
              <Input
                type="number"
                value={m}
                onChange={(e) => setM(e.target.value)}
                min="4"
                max="64"
              />
            </div>
            <div className="grid gap-2">
              <label className="text-sm font-medium">EF Construction</label>
              <Input
                type="number"
                value={efConstruction}
                onChange={(e) => setEfConstruction(e.target.value)}
                min="50"
                max="500"
              />
            </div>
          </div>
          <div className="grid gap-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">
                Initial Vectors (optional JSON array)
              </label>
              <div className="flex items-center gap-2">
                <Input
                  type="number"
                  value={demoCount}
                  onChange={(e) => setDemoCount(e.target.value)}
                  className="w-20 h-8 text-xs"
                  min="1"
                  max="10000"
                  placeholder="Count"
                />
                <span className="text-xs text-muted-foreground">x</span>
                <Input
                  type="number"
                  value={demoDimension}
                  onChange={(e) => setDemoDimension(e.target.value)}
                  className="w-20 h-8 text-xs"
                  min="1"
                  max="2048"
                  placeholder="Dim"
                />
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={handleGenerateDemo}
                >
                  <Sparkles className="mr-1 h-3 w-3" />
                  Generate
                </Button>
              </div>
            </div>
            <Textarea
              placeholder='[{"id": 1, "vector": [0.1, 0.2, ...]}, ...]'
              value={vectorsInput}
              onChange={(e) => setVectorsInput(e.target.value)}
              className="h-32 font-mono text-sm"
            />
            <p className="text-xs text-muted-foreground">
              Format: {`[{"id": number, "vector": number[]}, ...]`}
            </p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleCreate} disabled={createMutation.isPending}>
            {createMutation.isPending ? 'Creating...' : 'Create Index'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
