# Welcome to the End-to-End Stock Use Case developed in Microsoft Fabric

This GitHub repo contains all necessary items to create the End-to-End Stock Use Case in Microsoft Fabric. 

## Prerequisites

To make sure you can use this GitHub repo, a few things are required.

1. You need a Fabric Capacity
2. You need to enable some Tenant Settings
    * [Users can create Fabric items](https://learn.microsoft.com/en-us/fabric/admin/fabric-switch)
    * [Users can synchronize workspace items with their Git repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
    * [Users can sync workspaces items with GitHub repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
    * Optional: If you wish to activate a Fabric Trial, you need to enable [Users can try Microsoft Fabric paid features](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-self-service-signup-purchase-for-power-bi?tabs=free-sign-up)
3. You need a Workspace backed up by a Fabric Capacity with admin rights to connect your GitHub repo
4. You need a GitHub user


### 1. You need a Fabric Capacity
To be able to use this GitHub repo, a Fabric Capacity is required. You can either use a F2+, a Fabric Trial, or Power BI Premium. To activate a trial, you can follow [this link.](https://learn.microsoft.com/en-us/fabric/fundamentals/fabric-trial)

### 2. You need to enablel some Tenant settings
To make sure you can sync your workspace with GitHub, you need to enable following tenat settings:
* [Users can create Fabric items](https://learn.microsoft.com/en-us/fabric/admin/fabric-switch)
* [Users can synchronize workspace items with their Git repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
* [Users can sync workspaces items with GitHub repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
* Optional: If you wish to activate a Fabric Trial, you need to enable [Users can try Microsoft Fabric paid features](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-self-service-signup-purchase-for-power-bi?tabs=free-sign-up)

### 3. You need a Workspace backed up by a Fabric Capacity with admin rights to connect your GitHub repo
Make sure your Workspace is assigned to a Capacity - Fabric, Premium or Trial.

<img src="./PNG/01%20License%20Mode%20Fabric%20Capacity.png" width="500">

Further, make sure you have admin rights on the workspace.

### 4. You need a GitHub user
If you don't have a GitHub user already, please follow [this link](https://github.com/signup?source=login) to create one. In the following screen shots you'll see how to create one with a dummy account. 

Sign up using an email, providing a secure password, and a username

<img src="./PNG/02%20Sign%20up%20to%20GitHub.png" width="500">

Next, verfiy your account with one option.

<img src="./PNG/03%20Verify%20your%20account.png" width="500">



## Let's get started
Once all prerequisites are met, you can log in to GitHub and access following [GitHub Repo.](https://github.com/PBI-Guy/End-to-End-Stock-Use-Case-with-Fabric) From there, press the button to create a fork.

<img src="./PNG/05%20Create%20a%20GitHub%20Fork.png" width="500">

Choose the owner if you have multiple and give a new repo name if you wish. Per default, the upstream repo (Fabric-Event) is used. Once done, hit the **Create fork** button and wait until it's synced. 

<img src="./PNG/06%20Create%20Fork.png" width="500">

### Create a personal access token in GitHub
As a next step, we need to create a personal access token for our GitHub which will be used afterwards to sync our Workspace with the GitHub repo. To create one, click on the **top right user icon** and select **Settings**

<img src="./PNG/07%20GitHub%20User%20Settings.png" width="500">

Scroll down and select **Developer settings**. From there, choose **Personal access tokens** and select **Fine-grained tokens**. Lastly, click the green **Generate new token** button.

<img src="./PNG/08%20Create%20Personal%20Access%20Token%20GitHub.png" width="500">

Next, you need to provide a Token name, e.g. _Fabric Event_, and give access to the repository. Make sure to only give access to the Fabric-Event repo we just forked. 

<img src="./PNG/09%20Token%20repo%20access.png" width="500">

Make ksure to provide Read and Write permissions on Contents.

<img src="./PNG/9.1%20Token%20permission.png" width="500">

Once everything configured, press the **Generate token** button at the bottom of the page. A message will pop up summarizing the configuration. Confirm by pressing the **Generate token** button.


<img src="./PNG/10%20Token%20generation.png" width="500">

Make sure to copy and store the access token securely as you will not be able to see it again once you leave this page. 


<img src="./PNG/11%20Copy%20Access%20Token.png" width="500">

### Sync GitHub repo with a Fabric workspace

As a final step, we need to sync now the GitHub repo with our Workspace. Open Microsoft Fabric, navigate to your workspace and press the **Workspace settings** at the top right. Now, select Git integration, choose GitHub and press the **Add account** button.

<img src="./PNG/12%20Workspace%20Git%20Integration%20Config.png" width="500">

In the pop-up window, give a Display Name and past your GitHub Personal Access Token which we created before. Also, provide the GitHub repo URL which has following structure: https://github.com/_YourGitHubUser_/End-to-End-Stock-Use-Case-with-Fabric, e.g. https://github.com/PBI-Guy/End-to-End-Stock-Use-Case-with-Fabric Hit **Add**.

<img src="./PNG/13%20Add%20GitHub%20account.png" width="500">

After the account has been successfully added, you can select **Connect**. Now, you have to select the main branch, and add **Workspace Items to the Git folder**. Lasty, click **Connect and sync**.

<img src="./PNG/14%20Configure%20Repo.png" width="500">

Now, all Fabric items from the GitHub repo will be synced into the workspace. This can take a few minutes. You can follow the process at the top right corner.

Once all items have successfully synced, we need to configure a few of them and create two Pipelines to orchestrate and ingest data. 


## Configure all items

Let's start with our Spark Environment config by selecting the **ENV_Spark_Stock_Use_Case** item. 

<img src="./PNG/19%20Spark%20Environment.png" width="500">

In the navigation pane on the left hand side, select **Public Library** and hit **Add from PyPI**. Search for **yahoo-fin** and make sure the version is at least **0.8.9.1**. Add a new library by selecting the **+ Add from PyPI** button at the top and search for yfinance. Make sure the version is at least **0.2.55**. 

<img src="./PNG/20%20Add%20Public%20Libraries.png" width="500">

Now, hit the **Publish** button at the top right and confirm by selecting **Publish all**. In the confirmation screen hit again **Publish**. This will take a few minutes to process.

<img src="./PNG/21%20Publish%20Spark%20changes.png" width="500">

In our next step, we're going to configure our Notebooks. Return to the Workspace overview by selecting the name in the left navigation pane. Select **02 Get Company Details** to open it and select Lakehouses in the Explorer. 

<img src="./PNG/15%20Select%20Lakehouse.png" width="500">

You'll see a "Missing Lakehouse" followed by a number. Select the two arrows next to it and choose **Remove all lakehouses**. In the pop up window just confirm by hitting the **Continue** button.

<img src="./PNG/16%20Remove%20Missing%20Lakehouse.png" width="500">

Next, let's add our existing Lakehouse to the Notebook by selecting **+ Lakehouses** or the green **Add** button in the explorer. In the pop up window, select **Existing lakehouse(s) without Schema** and continue by pressing the **Add** button.

<img src="./PNG/17%20Add%20Lakehouse%20to%20Notebook.png" width="500">

In the next pop up window, select the LH_Stocks Lakehouse and hit **Add**. Now, let's run the Notebook manually to test if it works. Just hit the Run all button in the ribbon and wait until suceeded. Once finished, let's configure the other Notebook **01 Get Stock Details** by following the same steps to remove all existing Lakehouses connected to the Notebook and add the same Lakehouse to it. 


[asdf](./PNG/21%20Publish%20Spark%20changes.png)













_ThisItalic_

**bold**


`code` testing code
