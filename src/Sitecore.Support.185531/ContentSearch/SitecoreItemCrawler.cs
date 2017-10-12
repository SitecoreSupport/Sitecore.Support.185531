// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SitecoreItemCrawler.cs" company="Sitecore">
//   Copyright (c) Sitecore. All rights reserved.
// </copyright>
// <summary>
//   The sitecore item crawler.
// </summary>
// --------------------------------------------------------------------------------------------------------------------



namespace Sitecore.Support.ContentSearch
{
    using Sitecore.Configuration;
    using Sitecore.Data.LanguageFallback;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;

    using Sitecore.Collections;
    using Sitecore.Abstractions;

    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Pipelines.GetContextIndex;
    using Sitecore.ContentSearch.Utilities;
    using Sitecore.Data;
    using Sitecore.Data.Items;
    using Sitecore.Data.Managers;
    using Sitecore.Diagnostics;
    using Sitecore.SecurityModel;
    using Sitecore.ContentSearch;

    /// <summary>
    /// The sitecore item crawler.
    /// </summary>
    public class SitecoreItemCrawler : HierarchicalDataCrawler<SitecoreIndexableItem>, IContextIndexRankable
    {
        /****************************************************************
         * FIELDS
         ****************************************************************/

        /// <summary>
        /// The database.
        /// </summary>
        private string database;

        /// <summary>
        /// The root.
        /// </summary>
        private string root;

        /// <summary>
        /// The root item.
        /// </summary>
        private Item rootItem;

        /// <summary>
        /// Identifies if obtaining root item error was logged
        /// </summary>
        private volatile int rootItemErrorLogged;

        /****************************************************************
           * CONSTRUCTORS
           ****************************************************************/

        /// <summary>
        /// Initializes a new instance of the <see cref="SitecoreItemCrawler" /> class.
        /// </summary>
        public SitecoreItemCrawler()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SitecoreItemCrawler" /> class.
        /// </summary>
        /// <param name="indexOperations">The index operations.</param>
        public SitecoreItemCrawler(IIndexOperations indexOperations)
          : base(indexOperations)
        {
        }

        /****************************************************************
         * PROPERTIES
         ****************************************************************/

        /// <summary>
        /// Gets or sets the database.
        /// </summary>
        /// <value>
        /// The database.
        /// </value>
        public string Database
        {
            get
            {
                return string.IsNullOrEmpty(this.database) ? null : this.database;
            }

            set
            {
                this.database = value;
            }
        }

        /// <summary>
        /// Gets or sets the root.
        /// </summary>
        /// <value>
        /// The root.
        /// </value>
        public string Root
        {
            get
            {
                if (string.IsNullOrEmpty(this.root))
                {
                    var db = ContentSearchManager.Locator.GetInstance<IFactory>().GetDatabase(this.database);
                    Assert.IsNotNull(db, "Database " + this.database + " does not exist");
                    using (new SecurityDisabler())
                    {
                        this.root = db.GetRootItem().ID.ToString();
                    }
                }

                return this.root;
            }

            set
            {
                this.root = value;
                this.rootItem = null;
            }
        }

        /// <summary>
        /// Gets the root item.
        /// </summary>
        /// <value>
        /// The root item.
        /// </value>
        /// <exception cref="System.InvalidOperationException"></exception>
        public Item RootItem
        {
            get
            {
                this.rootItem = this.GetRootItem();

                if (this.rootItem == null)
                    throw new InvalidOperationException(string.Format("[Index={0}, Crawler={1}, Database={2}] Root item could not be found: {3}.", this.index != null ? this.index.Name : "NULL", typeof(SitecoreItemCrawler).Name, this.database, this.root));

                return this.rootItem;
            }
        }

        /// <summary>
        /// Gets the root item.
        /// </summary>
        /// <returns></returns>
        private Item GetRootItem()
        {
            if (this.rootItem == null)
            {
                var db = ContentSearchManager.Locator.GetInstance<IFactory>().GetDatabase(this.database);
                Assert.IsNotNull(db, "Database " + this.database + " does not exist");
                using (new SecurityDisabler())
                {
                    this.rootItem = db.GetItem(this.Root);

                    if (rootItem == null && rootItemErrorLogged == 0)
                    {
                        Interlocked.Increment(ref this.rootItemErrorLogged);
                        string message = string.Format("[Index={0}, Crawler={1}, Database={2}] Root item could not be found: {3}.", this.index != null ? this.index.Name : "NULL", typeof(SitecoreItemCrawler).Name, this.database, this.root);

                        CrawlingLog.Log.Error(message);
                        Log.Error(message, this);
                    }
                }
            }

            return this.rootItem;
        }

        /****************************************************************
         * HierarchicalCrawler
         ****************************************************************/

        /// <summary>Initializes the specified index.</summary>
        /// <param name="index">The index.</param>
        public override void Initialize(ISearchIndex index)
        {
            Assert.ArgumentNotNull(index, "index");
            Assert.IsNotNull(this.Database, "Database element not set.");
            Assert.IsNotNull(this.Root, "Root element not set.");

            if (this.Operations == null)
            {
                this.Operations = index.Operations;
                CrawlingLog.Log.Info(string.Format("[Index={0}] Initializing {3}. DB:{1} / Root:{2}", index.Name, this.Database, this.Root, typeof(SitecoreItemCrawler).Name));
            }

            base.Initialize(index);
        }

        public virtual int GetContextIndexRanking(IIndexable indexable)
        {
            var sitecoreIndexable = indexable as SitecoreIndexableItem;

            if (sitecoreIndexable == null)
                return int.MaxValue;

            if (this.GetRootItem() == null)
                return int.MaxValue;

            Item item = sitecoreIndexable;

            using (new SecurityDisabler())
            {
                using (new SitecoreCachesDisabler())
                {
                    int rank = item.Axes.Level - this.RootItem.Axes.Level;

                    return rank;
                }
            }
        }

        public override bool IsExcludedFromIndex(IIndexable indexable)
        {
            return this.IsExcludedFromIndex((SitecoreIndexableItem)indexable, true);
        }

        /// <summary>
        /// Updates the specified context.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="indexableUniqueId">The indexable unique identifier.</param>
        /// <param name="indexingOptions">The indexing options.</param>
        public override void Update(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions = IndexingOptions.Default)
        {
            this.Update(context, indexableUniqueId, null, indexingOptions);
        }

        /// <summary>
        /// Updates specific item.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="indexableUniqueId">The indexable unique id.</param>
        /// <param name="operationContext">The operation context.</param>
        /// <param name="indexingOptions">The indexing options.</param>
        public override void Update(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexEntryOperationContext operationContext, IndexingOptions indexingOptions = IndexingOptions.Default)
        {
            Assert.ArgumentNotNull(indexableUniqueId, "indexableUniqueId");

            var contextEx = context as ITrackingIndexingContext;
            var skipIndexable = contextEx != null && !contextEx.Processed.TryAdd(indexableUniqueId, null);

            if (skipIndexable || !ShouldStartIndexing(indexingOptions))
                return;

            var options = this.DocumentOptions;
            Assert.IsNotNull(options, "DocumentOptions");

            if (this.IsExcludedFromIndex(indexableUniqueId, operationContext, true))
                return;

            if (operationContext != null)
            {
                if (operationContext.NeedUpdateChildren)
                {
                    var item = Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);

                    if (item != null)
                    {
                        // check if we moved item out of the index's root.
                        bool needDelete = operationContext.OldParentId != Guid.Empty
                               && this.IsRootOrDescendant(new ID(operationContext.OldParentId))
                               && !this.IsAncestorOf(item);

                        if (needDelete)
                        {
                            this.Delete(context, indexableUniqueId);
                            return;
                        }

                        this.UpdateHierarchicalRecursive(context, item, CancellationToken.None);
                        return;
                    }
                }

                if (operationContext.NeedUpdatePreviousVersion)
                {
                    var item = Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);
                    if (item != null)
                    {
                        this.UpdatePreviousVersion(item, context);
                    }
                }
            }

            var indexable = this.GetIndexableAndCheckDeletes(indexableUniqueId);

            if (indexable == null)
            {
                if (this.GroupShouldBeDeleted(indexableUniqueId.GroupId))
                {
                    this.Delete(context, indexableUniqueId.GroupId);
                    return;
                }

                this.Delete(context, indexableUniqueId);
                return;
            }

            this.DoUpdate(context, indexable, operationContext);
        }

        protected override bool IsExcludedFromIndex(SitecoreIndexableItem indexable, bool checkLocation = false)
        {
            Item item = indexable;

            Assert.ArgumentNotNull(item, "item");
            var options = this.DocumentOptions;
            Assert.IsNotNull(options, "DocumentOptions");

            if (!item.Database.Name.Equals(this.Database, StringComparison.InvariantCultureIgnoreCase))
            {
                this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:excludedfromindex", this.index.Name, item.Uri);
                return true;
            }

            if (checkLocation)
            {
                if (GetRootItem() == null)
                    return true;

                if (!this.IsAncestorOf(item))
                {
                    this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:excludedfromindex", this.index.Name, item.Uri);
                    return true;
                }
            }


            if (options.HasIncludedTemplates)
            {

                if (options.IncludedTemplates.Contains(item.TemplateID.ToString()))
                {
                    return false;
                }
                this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:excludedfromindex", this.index.Name, item.Uri);
                return true;
            }

            if (options.ExcludedTemplates.Contains(item.TemplateID.ToString()))
            {
                this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:excludedfromindex", this.index.Name, item.Uri);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Determines whether the index root is an ancestor of the specified item.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns><c>true</c> if item is a descendant.</returns>
        protected virtual bool IsAncestorOf(Item item)
        {
            using (new SecurityDisabler())
            {
                using (new WriteCachesDisabler())
                {
                    if (this.RootItem != null)
                        return item.Paths.LongID.StartsWith(this.RootItem.Paths.LongID, StringComparison.InvariantCulture);
                }
            }

            return false;
        }

        /// <summary>
        /// Determines whether the indexable is excluded from index.
        /// </summary>
        /// <param name="indexableUniqueId">The indexable unique identifier.</param>
        /// <returns><c>true</c> if the indexable is excluded from index; otherwise false.</returns>
        protected override bool IsExcludedFromIndex(IIndexableUniqueId indexableUniqueId)
        {
            return this.IsExcludedFromIndex(indexableUniqueId, false);
        }

        /// <summary>
        /// Determines whether the indexable is excluded from index.
        /// </summary>
        /// <param name="indexableUniqueId">The indexable unique identifier.</param>
        /// <param name="checkLocation">if set to <c>true</c> then the check considers location of the indexable.</param>
        /// <returns><c>true</c> if the indexable is excluded from index; otherwise false.</returns>
        protected override bool IsExcludedFromIndex(IIndexableUniqueId indexableUniqueId, bool checkLocation)
        {
            return this.IsExcludedFromIndex(indexableUniqueId, null, checkLocation);
        }

        /// <summary>
        /// Determines whether the indexable is excluded from index.
        /// </summary>
        /// <param name="indexableUniqueId">The indexable unique identifier.</param>
        /// <param name="operationContext">The operation context.</param>
        /// <param name="checkLocation">if set to <c>true</c> then the check considers location of the indexable.</param>
        /// <returns><c>true</c> if the indexable is excluded from index; otherwise false.</returns>
        protected virtual bool IsExcludedFromIndex(IIndexableUniqueId indexableUniqueId, IndexEntryOperationContext operationContext, bool checkLocation)
        {
            ItemUri itemUri = indexableUniqueId as SitecoreItemUniqueId;

            if (itemUri != null && !itemUri.DatabaseName.Equals(this.Database, StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }

            if (!checkLocation)
            {
                return false;
            }

            if (operationContext != null
              && operationContext.OldParentId != Guid.Empty
              && this.IsRootOrDescendant(new ID(operationContext.OldParentId)))
            {
                return false;
            }

            Item item = Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);

            if (item != null && !this.IsAncestorOf(item))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Determines whether the id is equal to the root ID or is a descendant of the root.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns><c>true</c> if the id equals to root's ID or it is its descendant; otherwise, false.</returns>
        private bool IsRootOrDescendant(ID id)
        {
            if (this.RootItem.ID == id)
            {
                return true;
            }

            var factory = ContentSearchManager.Locator.GetInstance<IFactory>();
            Database db = factory.GetDatabase(this.Database);
            Item oldParent;
            using (new SecurityDisabler())
            {
                oldParent = db.GetItem(id);
            }

            if (oldParent != null && this.IsAncestorOf(oldParent))
            {
                return true;
            }

            return false;
        }

        protected override void DoAdd(IProviderUpdateContext context, SitecoreIndexableItem indexable)
        {
            Assert.ArgumentNotNull(context, "context");
            Assert.ArgumentNotNull(indexable, "indexable");

            using (new LanguageFallbackItemSwitcher(context.Index.EnableItemLanguageFallback))
            {
                this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:adding", context.Index.Name, indexable.UniqueId, indexable.AbsolutePath);
                if (!this.IsExcludedFromIndex(indexable))
                {
                    /*************************************************************/

                    foreach (var language in indexable.Item.Languages)
                    {
                        Item latestVersion;
                        using (new WriteCachesDisabler())
                        {
                            latestVersion = indexable.Item.Database.GetItem(indexable.Item.ID, language, Data.Version.Latest);
                        }

                        if (latestVersion == null)
                        {
                            CrawlingLog.Log.Warn(string.Format("SitecoreItemCrawler : AddItem : Could not build document data {0} - Latest version could not be found. Skipping.", indexable.Item.Uri));
                            continue;
                        }

                        Item[] versions;
                        using (new WriteCachesDisabler())
                        {
                            versions = latestVersion.Versions.GetVersions(false);
                        }

                        {
                            foreach (var version in versions)
                            {
                                var versionIndexable = (SitecoreIndexableItem)version;
                                var versionBuiltinFields = (IIndexableBuiltinFields)versionIndexable;

                                versionBuiltinFields.IsLatestVersion = versionBuiltinFields.Version == latestVersion.Version.Number;
                                versionIndexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;

                                this.Operations.Add(versionIndexable, context, this.index.Configuration);
                            }
                        }
                    }
                }

                /*************************************************************/

                this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:added", context.Index.Name, indexable.UniqueId, indexable.AbsolutePath);
            }
        }

        /// <summary>
        /// Executes the update event.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="indexable">The indexable.</param>
        protected override void DoUpdate(IProviderUpdateContext context, SitecoreIndexableItem indexable)
        {
            this.DoUpdate(context, indexable, null);
        }

        /// <summary>
        /// Executes the update event.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="indexable">The indexable.</param>
        /// <param name="operationContext">The operation context.</param>
        protected override void DoUpdate(IProviderUpdateContext context, SitecoreIndexableItem indexable, IndexEntryOperationContext operationContext)
        {
            Assert.ArgumentNotNull(context, "context");
            Assert.ArgumentNotNull(indexable, "indexable");

            using (new LanguageFallbackItemSwitcher(this.Index.EnableItemLanguageFallback))
            {
                if (this.IndexUpdateNeedDelete(indexable))
                {
                    this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:deleteitem", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
                    this.Operations.Delete(indexable, context);
                    return;
                }

                this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updatingitem", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
                if (!this.IsExcludedFromIndex(indexable, true))
                {
                    /*************************************************************/
                    if (operationContext != null && !operationContext.NeedUpdateAllVersions)
                    {
                        this.UpdateItemVersion(context, indexable, operationContext);
                    }
                    else
                    {
                        var languages = (operationContext != null && !operationContext.NeedUpdateAllLanguages) ? new[] { indexable.Item.Language } : indexable.Item.Languages;


                        foreach (var language in languages)
                        {
                            Item latestVersion;
                            using (new WriteCachesDisabler())
                            {
                                latestVersion = indexable.Item.Database.GetItem(indexable.Item.ID, language, Data.Version.Latest);
                            }

                            if (latestVersion == null)
                            {
                                CrawlingLog.Log.Warn(string.Format("SitecoreItemCrawler : Update : Latest version not found for item {0}. Skipping.", indexable.Item.Uri));
                                continue;
                            }

                            Item[] versions;
                            using (new SitecoreCachesDisabler())
                            {
                                versions = latestVersion.Versions.GetVersions(false);
                            }

                            foreach (var version in versions)
                            {
                                this.UpdateItemVersion(context, version, operationContext);
                            }
                        }
                    }

                    /*************************************************************/
                    this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updateditem", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
                }

                if (this.DocumentOptions.ProcessDependencies)
                {
                    this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updatedependents", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
                    this.UpdateDependents(context, indexable);
                }
            }
        }

        /// <summary>
        /// Updates item version.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="version">The item version.</param>
        [Obsolete("Use UpdateItemVersion(IProviderUpdateContext context, Item version, IndexEntryOperationContext operationContext) instead")]
        protected virtual void UpdateItemVersion(IProviderUpdateContext context, Item version)
        {
            this.UpdateItemVersion(context, version, new IndexEntryOperationContext());
        }

        protected virtual void UpdateItemVersion(IProviderUpdateContext context, Item version, IndexEntryOperationContext operationContext)
        {
            SitecoreIndexableItem versionIndexable = this.PrepareIndexableVersion(version, context);

            this.Operations.Update(versionIndexable, context, context.Index.Configuration);

            this.UpdateClones(context, versionIndexable);

            this.UpdateLanguageFallbackDependentItems(context, versionIndexable, operationContext);
        }

        /// <summary>
        /// Updates the clones of the version.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="versionIndexable">The version indexable.</param>
        private void UpdateClones(IProviderUpdateContext context, SitecoreIndexableItem versionIndexable)
        {
            IEnumerable<Item> clones;
            using (new WriteCachesDisabler())
            {
                clones = versionIndexable.Item.GetClones(false);
            }


            foreach (var clone in clones)
            {
                var cloneIndexable = PrepareIndexableVersion(clone, context);

                if (!this.IsExcludedFromIndex(clone))
                {
                    this.Operations.Update(cloneIndexable, context, context.Index.Configuration);
                }
            }
        }

        private void UpdateLanguageFallbackDependentItems(IProviderUpdateContext context, SitecoreIndexableItem versionIndexable, IndexEntryOperationContext operationContext)
        {
            if (operationContext == null || operationContext.NeedUpdateAllLanguages)
            {
                return;
            }

            var item = versionIndexable.Item;
            if (LanguageFallbackFieldSwitcher.CurrentValue != true)
            {
                if (LanguageFallbackItemSwitcher.CurrentValue != true)
                {
                    return;
                }

                if (StandardValuesManager.IsStandardValuesHolder(item) && item.Fields[FieldIDs.EnableItemFallback].GetValue(false) != "1")
                {
                    return;
                }

                using (new LanguageFallbackItemSwitcher(false))
                {
                    if (item.Fields[FieldIDs.EnableItemFallback].GetValue(true, true, false) != "1")
                    {
                        return;
                    }
                }
            }

            if (!item.Versions.IsLatestVersion())
            {
                return;
            }

            var sitecoreIndexableItems = this.GetItem(item).Select(item1 => this.PrepareIndexableVersion(item1, context)).ToList();
            sitecoreIndexableItems.ForEach(sitecoreIndexableItem => this.Operations.Update(sitecoreIndexableItem, context, context.Index.Configuration));
            this.RemoveOutdatedFallbackItem(item, context);
        }

        public override void Delete(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions = IndexingOptions.Default)
        {
            base.Delete(context, indexableUniqueId, indexingOptions);

            if (!context.Index.EnableItemLanguageFallback)
            {
                return;
            }
            //bug fix area
            ItemUri itemUri = (SitecoreItemUniqueId)indexableUniqueId;

            if (itemUri.DatabaseName != this.Database)
            {
                return;
            }

            var item = ItemManager.GetItem(itemUri.ItemID, itemUri.Language, itemUri.Version, ContentSearchManager.Locator.GetInstance<IFactory>().GetDatabase(itemUri.DatabaseName));
            if (item.Fields[FieldIDs.EnableItemFallback].GetValue(true, true, false) == "1")
            {
                this.DeleteFallbackItem(indexableUniqueId, context);
            }
        }

        /// <summary>
        /// Fallback language keep storing outdated version's index.
        /// Remove outdated version item from corresponding index.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="item">The version indexable.</param>
        private void RemoveOutdatedFallbackItem(Item item, IProviderUpdateContext context)
        {
            var items = this.GetItem(item).ToList();
            foreach (var sitecoreItem in items)
            {
                var currentVersion = sitecoreItem.Version.ToInt32();
                var effectiveVersion = currentVersion - 1;
                for (var i = effectiveVersion; i > 0; i--)
                {
                    var oldItem = ItemManager.GetItem(sitecoreItem.ID, sitecoreItem.Language, new Data.Version(i), item.Database);
                    if (oldItem != null && i != currentVersion)
                    {
                        var indexableItem = this.PrepareIndexableVersion(oldItem, context);
                        this.Operations.Delete(indexableItem, context);
                    }
                }
                var futureBackdatedItem = ItemManager.GetItem(sitecoreItem.ID, sitecoreItem.Language, new Data.Version(item.Version.ToInt32() + 1), item.Database);
                if (futureBackdatedItem != null && futureBackdatedItem.Version.ToInt32() != currentVersion)
                {
                    var indexableItem = this.PrepareIndexableVersion(futureBackdatedItem, context);
                    this.Operations.Delete(indexableItem, context);
                }

            }
        }

        /// <summary>
        /// When perform a version deletion operation,
        /// Make sure that all the fallback language version deleted if it is the last item of main language.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="id">The version indexable.</param>
        private void DeleteFallbackItem(IIndexableUniqueId id, IProviderUpdateContext context)
        {
            if (new ItemUri(id as SitecoreItemUniqueId).DatabaseName != this.Database)
            {
                return;
            }

            var item = Data.Database.GetItem(id as SitecoreItemUniqueId);
            if (item == null)
            {
                return;
            }
            var items = LanguageFallbackManager.GetDependentLanguages(item.Language, item.Database, item.ID)
              .Select(
                language =>
                {
                    var item1 = item.Database.GetItem(item.ID, language, item.Version);
                    return item1;
                });
            foreach (var indexableItem in items.Select(sitecoreItem => this.PrepareIndexableVersion(sitecoreItem, context)))
            {
                this.Operations.Delete(indexableItem, context);
            }
        }

        /// <summary>
        /// Get all the dependent language item for a item
        /// </summary>
        /// <param name="item">item which we need to know the dependent language</param>
        /// <returns>all the dependent language</returns>
        private IEnumerable<Item> GetItem(Item item)
        {
            var indexableItem = LanguageFallbackManager.GetDependentLanguages(item.Language, item.Database, item.ID)
              .SelectMany(
                language =>
                {
                    var item1 = item.Database.GetItem(item.ID, language);
                    var items = item1 != null ? item1.Versions.GetVersions() : new Item[0];
                    return items;
                })
              .Where(item1 => !this.IsExcludedFromIndex(item1));
            return indexableItem;
        }

        /// <summary>
        /// Prepares the indexable version.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="context">The context.</param>
        /// <returns>indexable item object</returns>
        internal SitecoreIndexableItem PrepareIndexableVersion(Item item, IProviderUpdateContext context)
        {
            var indexable = (SitecoreIndexableItem)item;
            var cloneBuiltinFields = (IIndexableBuiltinFields)indexable;
            cloneBuiltinFields.IsLatestVersion = item.Versions.IsLatestVersion();
            indexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;
            return indexable;
        }

        protected override SitecoreIndexableItem GetIndexable(IIndexableUniqueId indexableUniqueId)
        {
            using (new SecurityDisabler())
            {
                using (new WriteCachesDisabler())
                {
                    return Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);
                }
            }
        }

        /// <summary>
        /// Determines if the group should be deleted.
        /// </summary>
        /// <param name="indexableId">The indexable id.</param>
        /// <returns>true if the whole group should be deleted; otherwise false</returns>
        protected override bool GroupShouldBeDeleted(IIndexableId indexableId)
        {
            Assert.ArgumentNotNull(indexableId, "indexableId");
            var itemId = indexableId as SitecoreItemId;
            if (itemId == null)
            {
                return false;
            }

            Database db = Factory.GetDatabase(this.Database);
            Item item;
            using (new WriteCachesDisabler())
            {
                item = db.GetItem(itemId);
            }

            return item == null;
        }

        protected override SitecoreIndexableItem GetIndexableAndCheckDeletes(IIndexableUniqueId indexableUniqueId)
        {
            ItemUri itemUri = indexableUniqueId as SitecoreItemUniqueId;

            using (new SecurityDisabler())
            {
                Item item;
                using (new WriteCachesDisabler())
                {
                    item = Data.Database.GetItem(itemUri);
                }

                if (item != null)
                {
                    var latestItemUri = new ItemUri(itemUri.ItemID, itemUri.Language, Data.Version.Latest, itemUri.DatabaseName);
                    var latestItem = Data.Database.GetItem(latestItemUri);

                    Data.Version[] versions;
                    using (new WriteCachesDisabler())
                    {
                        versions = latestItem.Versions.GetVersionNumbers() ?? new Data.Version[0];
                    }

                    if (itemUri.Version != Data.Version.Latest && versions.All(v => v.Number != itemUri.Version.Number))
                        item = null;
                }

                return item;
            }
        }

        protected override bool IndexUpdateNeedDelete(SitecoreIndexableItem indexable)
        {
            return false;
        }

        protected override IEnumerable<IIndexableUniqueId> GetIndexablesToUpdateOnDelete(IIndexableUniqueId indexableUniqueId)
        {
            var itemUri = indexableUniqueId.Value as ItemUri;

            using (new SecurityDisabler())
            {
                var latestItemUri = new ItemUri(itemUri.ItemID, itemUri.Language, Data.Version.Latest, itemUri.DatabaseName);

                Item latestItem;
                using (new WriteCachesDisabler())
                {
                    latestItem = Data.Database.GetItem(latestItemUri);
                }

                if (latestItem != null && latestItem.Version.Number < itemUri.Version.Number)
                    yield return new SitecoreItemUniqueId(latestItem.Uri);
            }
        }

        public override SitecoreIndexableItem GetIndexableRoot()
        {
            using (new SecurityDisabler())
            {
                return this.RootItem;
            }
        }

        protected override IEnumerable<IIndexableId> GetIndexableChildrenIds(SitecoreIndexableItem parent)
        {
            var childList = this.GetChildList(parent.Item);

            if (childList.Count == 0)
                return null;

            return childList.Select(i => (SitecoreItemId)i.ID);
        }

        [CanBeNull]
        protected override IEnumerable<SitecoreIndexableItem> GetIndexableChildren(SitecoreIndexableItem parent)
        {
            var childList = this.GetChildList(parent.Item);

            if (childList.Count == 0)
                return null;

            return childList.Select(i => (SitecoreIndexableItem)i);
        }

        /// <summary>
        /// Gets the child list.
        /// </summary>
        /// <param name="parent">The parent.</param>
        /// <returns>Returns list of childs items.</returns>
        protected virtual ChildList GetChildList(Item parent)
        {
            ChildList childList;
            using (new WriteCachesDisabler())
            {
                childList = parent.GetChildren(ChildListOptions.IgnoreSecurity | ChildListOptions.SkipSorting);
            }

            return childList;
        }

        protected override SitecoreIndexableItem GetIndexable(IIndexableId indexableId, CultureInfo culture)
        {
            using (new SecurityDisabler())
            using (new WriteCachesDisabler())
            {
                var language = LanguageManager.GetLanguage(culture.Name, this.RootItem.Database);
                return ItemManager.GetItem(indexableId as SitecoreItemId, language, Data.Version.Latest, this.RootItem.Database, SecurityCheck.Disable);
            }
        }

        /// <summary>
        /// Updates the previous version.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="context">The context.</param>
        private void UpdatePreviousVersion(Item item, IProviderUpdateContext context)
        {
            Data.Version[] versions;
            using (new WriteCachesDisabler())
            {
                versions = item.Versions.GetVersionNumbers() ?? new Data.Version[0];
            }

            int indexOfItem = versions.ToList().FindIndex(version => version.Number == item.Version.Number);
            if (indexOfItem < 1)
            {
                return;
            }

            var previousVersion = versions[indexOfItem - 1];

            var previousItemVersion = versions.FirstOrDefault(version => version == previousVersion);
            var previousItemUri = new ItemUri(item.ID, item.Language, previousItemVersion, item.Database.Name);
            var previousItem = Data.Database.GetItem(previousItemUri);
            var versionIndexable = (SitecoreIndexableItem)previousItem;

            if (versionIndexable != null)
            {
                var versionBuiltinFields = (IIndexableBuiltinFields)versionIndexable;
                versionBuiltinFields.IsLatestVersion = false;
                versionIndexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;

                this.Operations.Update(versionIndexable, context, this.index.Configuration);
            }
        }
    }

    /// <summary>
    /// The write caches disabler.
    /// </summary>
    public class WriteCachesDisabler : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WriteCachesDisabler"/> class.
        /// </summary>
        public WriteCachesDisabler()
        {
            CacheWriteDisabler.Enter(Settings.GetBoolSetting("ContentSearch.Indexing.DisableDatabaseCaches", false));
        }

        /// <summary>
        /// The dispose.
        /// </summary>
        public void Dispose()
        {
            CacheWriteDisabler.Exit();
        }
    }
}
