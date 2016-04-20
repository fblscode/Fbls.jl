abstract Tbl

typealias TblCols Dict{Symbol, AnyCol}
typealias TblRecs Dict{RecId, Rec} 

immutable BasicTbl <: Tbl
    name::Symbol
    cols::TblCols
    recs::TblRecs
    timestamp::Col{DateTime}
    revision::Col{Int64}
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicTbl(n::Symbol) = begin
        t = new(n, 
                TblCols(),
                TblRecs(), 
                Col(DateTime, symbol("($n)_timestamp")), 
                Col(Revision, symbol("$(n)_revision")),
                Evt{Tuple{RecId}}(),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec}}())
        
        pushcol!(t, recid, t.timestamp, t.revision)
        return t
    end
end

Tbl(n::Symbol) = BasicTbl(n)

cols(tbl::Tbl) = values(BasicTbl(tbl).cols)

defname(tbl::Tbl) = BasicTbl(tbl).name

empty!(tbl::Tbl) = empty!(BasicTbl(tbl).recs)

findcol(tbl::Tbl, n::Symbol) = begin
    bt = BasicTbl(tbl)

    return if haskey(bt.cols, n) 
        Nullable{AnyCol}(bt.cols[n]) 
    else 
        Nullable{AnyCol}() 
    end
end

haskey(tbl::Tbl, id::RecId) = haskey(BasicTbl(tbl).recs, id)

timestamp(tbl::Tbl) = BasicTbl(tbl).timestamp

revision(tbl::Tbl) = BasicTbl(tbl).revision

delete!(tbl::Tbl, id::RecId) = begin
    bt = BasicTbl(tbl)
    if !haskey(tbl, id) throw(RecNotFound()) end
    push!(bt.ondelete, (id,))
    delete!(bt.recs, id)
end

get(tbl::Tbl, id::RecId) = begin
    if !haskey(tbl, id) throw(RecNotFound()) end
    return BasicTbl(tbl).recs[id]
end

initrec!(rec) = begin
    if !haskey(rec, recid) rec[recid] = RecId() end
    return rec
end

initrec!(tbl::Tbl, rec::Rec) = begin
    initrec!(rec)
    bt = BasicTbl(tbl)
    if !haskey(rec, bt.revision) rec[bt.revision] = 1 end
    return rec
end

upsert!(tbl::Tbl, rec::Rec) = begin
    bt = BasicTbl(tbl)
    id = get(rec, recid, nothing)
    rec[bt.timestamp] = now()

    prev = if id != nothing && haskey(bt.recs, id) 
        rec[bt.revision] += 1
        push!(bt.onupsert, (rec,))
        bt.recs[id]
    else 
        initrec!(bt, rec)
        id = rec[recid]
        push!(bt.onupsert, (rec,))
        bt.recs[id] = Rec() 
    end

    for c in values(bt.cols)
        if haskey(rec, c)
            f = Fld(c)
            prev[f] = rec[f]
        end
    end
    
    return rec
end

isdirty(rec::Rec, tbl::Tbl, cols::AnyCol...) = begin
    bt = BasicTbl(tbl)
    if !haskey(rec, recid) return true end
    rid = rec[recid]
    if !haskey(bt.recs, rid) return true end
    trec = bt.recs[rid]
    if isempty(cols) cols = bt.cols end
    return reduce(|, [get(rec, c, nothing) != get(trec, c, nothing) 
                      for c in cols])  
end

load!(tbl::Tbl, rec::Rec) = begin
    bt = BasicTbl(tbl)

    if haskey(rec, isdelCol)
        id = rec[recid]
        push!(bt.ondelete, (id,))
        delete!(bt.recs, id)
    else
        push!(bt.onload, (rec,))
        bt.recs[rec[recid]] = rec
    end

    return rec
end

ondelete(tbl::Tbl) = BasicTbl(tbl).ondelete 
onupsert(tbl::Tbl) = BasicTbl(tbl).onupsert 
onload(tbl::Tbl) = BasicTbl(tbl).onload

pushcol!(tbl::Tbl, cols::AnyCol...) = begin
    bt = BasicTbl(tbl)
    for c in cols bt.cols[defname(c)] = c end
end

done(tbl::Tbl, i) = done(values(BasicTbl(tbl).recs), i)
isempty(tbl::Tbl) = isempty(values(BasicTbl(tbl).recs))
length(tbl::Tbl) = length(values(BasicTbl(tbl).recs))
next(tbl::Tbl, i) = next(values(BasicTbl(tbl).recs), i)
start(tbl::Tbl) = start(values(BasicTbl(tbl).recs))

dump(tbl::Tbl, out::IOBuf) = for r in tbl writerec(tbl, r, out) end

load!(tbl::Tbl, in::IOBuf) = while !eof(in) load!(tbl, readrec(tbl, in)) end
