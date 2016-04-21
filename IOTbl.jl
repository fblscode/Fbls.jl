immutable IOTbl <: Tbl
    wrapped::Tbl
    buf::IO
    offs::Col{Offs}
    prevoffs::Col{Offs}
    
    IOTbl(tbl::Tbl, buf::IO, offs::Col{Offs}) = begin
        t = new(tbl, buf, offs, Col(Offs, symbol("$(defname(tbl))_prevoffs")))

        pushcol!(tbl, isdelCol, t.offs, t.prevoffs)

        return t
    end
end

IO(tbl::Tbl, buf::IO; offs::Col{Offs} = 
   Col(Offs, symbol("$(defname(tbl))_offs"))) = 
    IOTbl(tbl, buf, offs)

cols(tbl::IOTbl) = cols(tbl.wrapped)

convert(::Type{BasicTbl}, tbl::IOTbl) = BasicTbl(tbl.wrapped)

writerec(tbl::Tbl, rec::Rec) = begin
    iot = IOTbl(tbl)
    seekend(iot.buf)
    writerec(iot, rec, iot.buf)
end

delete!(tbl::IOTbl, id::RecId) = begin
    delete!(tbl.wrapped, id)
    writerec(tbl, RecOf(recid => id, isdelCol => true))
end

upsert!(tbl::IOTbl, rec::Rec) = begin
    if !haskey(rec, tbl.offs) rec[tbl.offs] = -1 end
    rec[tbl.prevoffs] = rec[tbl.offs]
    rec[tbl.offs] = position(tbl.buf)
    res = upsert!(tbl.wrapped, rec)
    writerec(tbl, rec)

    return res
end

offs(tbl::Tbl) = IOTbl(tbl).offs
prevoffs(tbl::Tbl) = IOTbl(tbl).prevoffs
