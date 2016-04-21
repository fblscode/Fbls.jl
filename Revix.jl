abstract Revix{V}

typealias RevixRecs{V} Dict{RecId, V}

immutable BasicRevix{V} <: Revix{V}
    name::Symbol
    col::Col{V}
    recs::RevixRecs{V}
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicRevix(n::Symbol, c::Col{V}) = new(n, c, 
                                              RevixRecs{V}(), 
                                              Evt{Tuple{RecId}}(),
                                              Evt{Tuple{Rec}}(),
                                              Evt{Tuple{Rec}}())
end

Revix{V}(n::Symbol, c::Col{V}) = BasicRevix{V}(n, c)

defname(rx::Revix) = BasicRevix(rx).name

empty!(rx::Revix) = empty!(BasicRevix(rx).recs)

get{V}(rx::Revix{V}, id::RecId) = BasicRevix{V}(rx).recs[id]

haskey(rx::Revix, id::RecId) = haskey(BasicRevix(rx).recs, id)

delete!(rx::Revix, id::RecId) = begin
    brx = BasicRevix(rx)
    if !haskey(rx, id) throw(RecNotFound()) end
    push!(brx.ondelete, (id,))
    delete!(brx.recs, id)
end

upsert!(rx::Revix, rec::Rec) = begin
    brx = BasicRevix(rx)
    push!(brx.onupsert, (rec,))
    brx.recs[rec[recid]] = rec[brx.col]
    return rec
end

load!(rx::Revix, rec::Rec) = begin
    brx = BasicRevix(rx)
    push!(brx.onload, (rec,))
    brx.recs[rec[recid]] = rec[brx.col]
    return rec
end

ondelete(rx::Revix) = BasicRevix(rx).ondelete
onupsert(rx::Revix) = BasicRevix(rx).onupsert 
onload(rx::Revix) = BasicRevix(rx).onload

done(rx::Revix, i) = done(BasicRevix(rx).recs, i)
isempty(rx::Revix) = isempty(BasicRevix(rx).recs)
length(rx::Revix) = length(BasicRevix(rx).recs)
next(rx::Revix, i) = next(BasicRevix(rx).recs, i)
start(rx::Revix) = start(BasicRevix(rx).recs)

dump(rx::Revix, out::IO) = begin
    brx = BasicRevix(rx)

    for (id, val) in brx.recs
        write(out, id.value)
        writeval(brx.col, val, out)
    end
end

load!(rx::Revix, in::IO) = begin
    brx = BasicRevix(rx)

    while !eof(in)
        id = readval(UUID, -1, in)
        s = readsize(in)

        if s == -1
            delete!(brx.recs, id)
        else
            brx.recs[id] = readval(brx.col, s, in)
        end
    end
end

immutable IORevix{V} <: Revix{V}
    wrapped::Revix{V}
    buf::IO
    
    IORevix(rx::Revix{V}, buf::IO) = new(rx, buf)
end

IO{V}(rx::Revix{V}, buf::IO) =
    IORevix{V}(rx, buf)

convert(::Type{BasicRevix}, rx::IORevix) = BasicRevix(rx.wrapped)

convert{V}(::Type{BasicRevix{V}}, rx::IORevix{V}) = 
    BasicRevix{V}(rx.wrapped)

delete!(rx::IORevix, id::RecId) = begin
    delete!(rx.wrapped, id)
    seekend(rx.buf)
    write(rx.buf, id.value)
    write(rx.buf, ValSize(-1)) 
end

upsert!(rx::IORevix, rec::Rec) = begin
    upsert!(rx.wrapped, rec)
    seekend(rx.buf)
    write(rx.buf, rec[recid].value)
    col = BasicRevix(rx).col
    writeval(col, rec[col], rx.buf)
    return rec
end
