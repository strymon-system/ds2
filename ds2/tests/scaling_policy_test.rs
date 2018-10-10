#[cfg(test)]
extern crate ds2;

use std::f64;
use ds2::dataflow::topology::*;
use ds2::policy::scaling::*;

#[test]
/// Creates a topology with one source, add rates for some epochs,
/// evaluates policy, and validates the results
fn evaluate_scaling_policy_one_source()
{
    let mut topo = Topology::new();
    let s = topo.add_unique_node("Source","Source",1);
    let op_1 = topo.add_unique_node("Op1","Op1",1);
    let op_2 = topo.add_unique_node("Op2","Op2",1);
    topo.add_unique_edge(s,op_1);
    topo.add_unique_edge(op_1,op_2);
    { // Set aggregated rates for epoch 0
        let lg = &mut topo.logical_graph;
        lg[s].rates.insert(0,(0.0,100.0,0.0,90.0));
        lg[op_1].rates.insert(0,(50.0,400.0,45.0,380.0));
        lg[op_2].rates.insert(0,(100.0,200.0,100.0,200.0));
    }
    let c0 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,0,true);
    { // Verify results for epoch 0
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),2);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),8);
    }
    assert_eq!(c0,"Op1,2,Op2,8,Source,1");
    { // Set aggregated rates for epoch 1
        let lg = &mut topo.logical_graph;
        lg[s].rates.insert(1,(0.0,200.0,0.0,90.0));
        lg[op_1].rates.insert(1,(40.0,700.0,15.0,380.0));
        lg[op_2].rates.insert(1,(99.0,200.0,100.0,200.0));
    }
    // Re-evaluate policy for epoch 0
    let c0 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,0,true);
    { // Verify results for epoch 0
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),2);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),8);
    }
    assert_eq!(c0,"Op1,2,Op2,8,Source,1");
    // Now evaluate policy for epoch 1
    let c1 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,1,true);
    { // Verify results for epoch 1
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&1).expect("Did not find optimal parallelism."),5);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&1).expect("Did not find optimal parallelism."),36);
    }
    assert_eq!(c1,"Op1,5,Op2,36,Source,1");
    { // Set aggregated rates for epoch 7
        let lg = &mut topo.logical_graph;
        lg[s].rates.insert(7,(0.0,200.0,0.0,90.0));
        lg[op_1].rates.insert(7,(10.0,700.0,15.0,380.0));
        lg[op_2].rates.insert(7,(201.0,200.0,100.0,200.0));
    }
    // Now evaluate policy for epoch 7
    let c7 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,7,true);
    { // Verify results for epoch 7
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&7).expect("Did not find optimal parallelism."),20);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&7).expect("Did not find optimal parallelism."),70);
    }
    assert_eq!(c7,"Op1,20,Op2,70,Source,1");
    assert_eq!(as_vec(&c7),vec![("Op1".to_string(),20),("Op2".to_string(),70),("Source".to_string(),1)]);
}

#[test]
/// Creates a topology with two sources, add rates for some epochs,
/// evaluates policy, and validates the results
fn evaluate_scaling_policy_two_sources()
{
    let mut topo = Topology::new();
    let s_1 = topo.add_unique_node("Source1","Source1",1);
    let s_2 = topo.add_unique_node("Source2","Source2",1);
    let op_1 = topo.add_unique_node("Op1","Op1",1);
    let op_2 = topo.add_unique_node("Op2","Op2",1);
    topo.add_unique_edge(s_1,op_1);
    topo.add_unique_edge(s_2,op_1);
    topo.add_unique_edge(op_1,op_2);
    { // Set aggregated rates for epoch 0
        let lg = &mut topo.logical_graph;
        lg[s_1].rates.insert(0,(0.0,100.0,0.0,90.0));
        lg[s_2].rates.insert(0,(0.0,100.0,0.0,90.0));
        lg[op_1].rates.insert(0,(50.0,400.0,45.0,380.0));
        lg[op_2].rates.insert(0,(100.0,200.0,100.0,200.0));
    }
    let c0 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,0,true);
    { // Verify results for epoch 0
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),4);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),16);
    }
    let mut c = as_vec(&c0);
    c.sort();
    let mut expected = vec![("Op1".to_string(),4),("Op2".to_string(),16),("Source2".to_string(),1),("Source1".to_string(),1)];
    expected.sort();
    assert_eq!(c,expected);
    { // Set aggregated rates for epoch 1
        let lg = &mut topo.logical_graph;
        lg[s_1].rates.insert(1,(0.0,200.0,0.0,90.0));
        lg[s_2].rates.insert(1,(0.0,200.0,0.0,90.0));
        lg[op_1].rates.insert(1,(40.0,700.0,15.0,380.0));
        lg[op_2].rates.insert(1,(99.0,200.0,100.0,200.0));
    }
    // Re-evaluate policy for epoch 0
    let c0 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,0,true);
    { // Verify results for epoch 0
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),4);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&0).expect("Did not find optimal parallelism."),16);
    }
    let mut c = as_vec(&c0);
    c.sort();
    let mut expected = vec![("Op1".to_string(),4),("Op2".to_string(),16),("Source2".to_string(),1),("Source1".to_string(),1)];
    expected.sort();
    assert_eq!(c,expected);
    // Now evaluate policy for epoch 1
    let c1 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,1,true);
    { // Verify results for epoch 1
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&1).expect("Did not find optimal parallelism."),10);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&1).expect("Did not find optimal parallelism."),71);
    }
    let mut c = as_vec(&c1);
    c.sort();
    let mut expected = vec![("Op1".to_string(),10),("Op2".to_string(),71),("Source2".to_string(),1),("Source1".to_string(),1)];
    expected.sort();
    assert_eq!(c,expected);
    { // Set aggregated rates for epoch 7
        let lg = &mut topo.logical_graph;
        lg[s_1].rates.insert(7,(0.0,200.0,0.0,90.0));
        lg[s_2].rates.insert(7,(0.0,200.0,0.0,90.0));
        lg[op_1].rates.insert(7,(10.0,700.0,15.0,380.0));
        lg[op_2].rates.insert(7,(201.0,200.0,100.0,200.0));
    }
    // Now evaluate policy for epoch 7
    let c7 = evaluate_scaling_policy_at_epoch(&mut topo,f64::MAX,0.0,7,true);
    { // Verify results for epoch 7
        let lg = &mut topo.logical_graph;
        assert_eq!(*lg[op_1].optimal_parallelism_per_epoch.get(&7).expect("Did not find optimal parallelism."),40);
        assert_eq!(*lg[op_2].optimal_parallelism_per_epoch.get(&7).expect("Did not find optimal parallelism."),140);
    }
    let mut conf = as_vec(&c7);
    conf.sort();
    let mut expected = vec![("Op1".to_string(),40),("Op2".to_string(),140),("Source2".to_string(),1),("Source1".to_string(),1)];
    expected.sort();
    assert_eq!(conf,expected);
}

#[test]
/// Tests whether dataflow configurations are set and retrieved properly
fn set_and_get_configuration()
{
    let mut topo = Topology::new();
    let s_1 = topo.add_unique_node("Source1","Source1",1);
    let s_2 = topo.add_unique_node("Source2","Source2",1);
    let op_1 = topo.add_unique_node("Op1","Op1",1);
    let op_2 = topo.add_unique_node("Op2","Op2",1);
    topo.add_unique_edge(s_1,op_1);
    topo.add_unique_edge(s_2,op_1);
    topo.add_unique_edge(op_1,op_2);
    // Set configuration
    let mut conf = vec![("Op1".to_string(),40),("Op2".to_string(),140)];
    topo.set_configuration(&conf);
    conf.sort();
    let mut retrieved_conf = topo.get_configuration(true);  // Ignore source operators
    retrieved_conf.sort();
    assert_eq!(conf,retrieved_conf);
    // Set configuration
    let mut conf = vec![("Source1".to_string(),1),("Source2".to_string(),1),("Op1".to_string(),40),("Op2".to_string(),140)];
    topo.set_configuration(&conf);
    conf.sort();
    let mut retrieved_conf = topo.get_configuration(false); // Include source operators
    retrieved_conf.sort();
    assert_eq!(conf,retrieved_conf);
}
