package optimizer;

import QueryStuff.Query;
import optimizer.Replication.BorderReplicationSource;

public class SourceSelection {

    public final static int BORDER_REPLICATION = 0;
    public final static int LOCAL_INDEX = 1;

    public int type;
    public Query query;
    public BorderReplicationSource borderReplicationSource;



    private SourceSelection(Query query , BorderReplicationSource borderReplicationSource ){
        this.query = query;
        this.borderReplicationSource = borderReplicationSource;
        if(query == null)
            type = BORDER_REPLICATION;
        else
            type = LOCAL_INDEX;
    }

    private SourceSelection(int type){
        this.type = type;
    }

    public static SourceSelection getLocalIndexInstance(Query query) {
        SourceSelection s =  new SourceSelection(LOCAL_INDEX);
        s.query = query;
        return s;
    }

    public static SourceSelection getBorderReplicationInstance(BorderReplicationSource borderReplicationSource){
        SourceSelection s =  new SourceSelection(BORDER_REPLICATION);
        s.borderReplicationSource = borderReplicationSource ;
        return s;
    }
}
