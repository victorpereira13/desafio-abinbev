# Project Setup Guide

## AWS EC2 Setup

1. **Log in to the AWS Management Console**:
   - Navigate to AWS Management Console.

2. **Navigate to the EC2 service**:
   - In the AWS Management Console, select "EC2" from the services menu.

3. **Launch a new EC2 instance**:
   - Click “Launch Instance” and follow the prompts to create a new EC2 instance.

4. **Configure security groups**:
   - Ensure necessary inbound and outbound traffic is allowed.

5. **SSH into the instance**:
   - Use your preferred SSH client to connect to the instance.

## Azure Blob Storage Setup

### Create a Storage Account

1. **Sign in to the Azure portal**:
   - Go to Azure Portal.

2. **Navigate to Storage Accounts**:
   - In the left-hand menu, select “Storage accounts”.

3. **Create a new storage account**:
   - Click on “Create”.
   - Fill in the required details:
     - **Subscription**: Select your subscription.
     - **Resource group**: Create a new resource group or select an existing one.
     - **Storage account name**: Enter a unique name for your storage account.
     - **Region**: Choose the region closest to your users.
     - **Performance**: Select “Standard” for most use cases.
     - **Replication**: Choose the replication option that suits your needs (e.g., LRS, GRS).
   - Click “Review + create” and then “Create”.

### Create a Blob Container

1. **Navigate to your storage account**:
   - Once the storage account is created, go to the storage account overview.

2. **Create a container**:
   - Click on “Containers” under the “Data storage” section.
   - Click on “+ Container”.
   - Enter a name for your container (e.g., “bronze”, “silver”, “gold”).
   - Set the public access level (e.g., “Private” for secure access).
   - Click “Create”.

### Configure Access and Permissions

This step is crucial as it enables Databricks to communicate with Azure Blob Storage.

1. **Access keys**:
   - In the storage account overview, click on “Access keys” under the “Security + networking” section.
   - Copy the connection string or access keys.

## Create a Databricks Workspace

1. **Sign in to the Azure portal**:
   - Go to Azure Portal.

2. **Navigate to Create a Resource**:
   - In the left-hand menu, select “Create a resource”.

3. **Search for Azure Databricks**:
   - In the search bar, type “Azure Databricks” and select it from the results.

4. **Create the Workspace**:
   - Click “Create”.
   - Fill in the required details:
     - **Subscription**: Select your subscription.
     - **Resource group**: Create a new resource group or select an existing one.
     - **Workspace name**: Enter a unique name for your Databricks workspace.
     - **Region**: Choose the region closest to your users.
     - **Pricing Tier**: Select the pricing tier that suits your needs (Standard, Premium, or Trial).
   - Click “Review + create” and then “Create”.

### Launch the Databricks Workspace

1. **Navigate to the Databricks workspace**:
   - Once the workspace is created, go to the resource.

2. **Launch Workspace**:
   - Click on “Launch Workspace” to open the Databricks UI.

### Create a Cluster

1. **Navigate to Clusters**:
   - In the Databricks workspace, click on “Clusters” in the left-hand menu.

2. **Create a Cluster**:
   - Click on “Create Cluster”.
   - Fill in the required details:
     - **Cluster name**: Enter a name for your cluster.
     - **Cluster mode**: Choose between Standard or High Concurrency.
     - **Databricks Runtime Version**: Select the runtime version you need.
     - **Autoscaling**: Enable or disable autoscaling based on your needs.
     - **Worker Type**: Choose the instance type for your workers.
     - **Number of Workers**: Specify the number of worker nodes.
   - Click “Create Cluster”.

### Create a Notebook

1. **Navigate to Workspace**:
   - In the left-hand menu, click on “Workspace”.

2. **Create a Notebook**:
   - Click on the dropdown next to your workspace name and select “Create” > “Notebook”.
   - Enter a name for your notebook.
   - Choose the default language (Python, Scala, SQL, or R).
   - Select the cluster you created earlier.
   - Click “Create”.
