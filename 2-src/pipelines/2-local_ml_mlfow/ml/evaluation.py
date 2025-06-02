def get_feature_importance(model, feature_columns):
    """
    Extract and display feature importance
    """
    print("📈 ANALYZING FEATURE IMPORTANCE...")
    
    try:
        # Get the GBT model (last stage in pipeline)
        gbt_model = model.stages[-1]
        importance_scores = gbt_model.featureImportances.toArray()
        
        # Create importance dictionary
        importance_dict = dict(zip(feature_columns, importance_scores))
        
        # Sort by importance
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        
        print(f"🔝 TOP 10 MOST IMPORTANT FEATURES:")
        for i, (feature, score) in enumerate(sorted_features[:10], 1):
            print(f"   {i:2d}. {feature:<25} = {score:.4f}")
        
        return importance_dict
        
    except Exception as e:
        print(f"❌ Could not extract feature importance: {e}")
        return {}
