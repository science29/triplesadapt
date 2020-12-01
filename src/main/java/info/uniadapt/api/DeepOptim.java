package info.uniadapt.api;

import QueryStuff.QueryWorkersPool;
import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import optimizer.Optimizer2;
import optimizer.Rules.GeneralReplicationRule;
import optimizer.Rules.OperationalRule;

import java.util.HashMap;

public class DeepOptim {


    public DeepOptim(QueryWorkersPool queryWorkersPool, IndexesPool indexPool, Dictionary dictionary, Transporter transporter, HashMap<Integer, Boolean> borderTripleMap) {
    }

    public void setGeneralReplicationRule(GeneralReplicationRule generalReplicationRule) {
    }

    public OperationalRule.TripleBlock getNextTriplesBlock() {
        return null;
    }

    public void setOptimizerRelax(Optimizer2 optimizer) {
    }
}
